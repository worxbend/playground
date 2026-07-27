/*
 * Copyright (c) 2020 Kzonix Projects
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package io.kzonix.cobalt

import io.kzonix.eventing.DeadLetter
import io.kzonix.eventing.DecodeFailure
import io.kzonix.eventing.KafkaCodecs
import io.kzonix.eventing.KafkaTrace
import io.kzonix.kernel.event.Envelope
import io.kzonix.kernel.event.Source
import io.kzonix.observability.LogContext
import io.kzonix.persistence.repository.NewEvent
import io.opentelemetry.api.trace.Tracer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.pekko.kafka.ConsumerMessage.Committable
import org.apache.pekko.kafka.ConsumerMessage.CommittableMessage
import scala.util.control.NonFatal

/** A record that decoded, together with everything the write path needs.
  *
  * The original `ConsumerRecord` is kept even though the [[Envelope]] is fully decoded, because a record that the
  * *database* later rejects still has to become a [[DeadLetter]], and a dead letter is only replayable if it carries
  * the original bytes and headers.
  */
final case class PendingWrite(
  record: ConsumerRecord[String, Array[Byte]],
  envelope: Envelope,
  event: NewEvent
)

/** One consumed record, decoded, with its offset still attached.
  *
  * `outcome` is an `Either` and never an exception, which is the whole point of decoding in a stream stage rather than
  * in a `Deserializer` (ADR §4.3): a throwing deserializer throws inside `KafkaConsumer.poll`, before the connector
  * sees the record, so the stream dies with the offset uncommitted and every restart replays the same poison record
  * forever.
  *
  * The `committable` travels with the record all the way to the commit stage. It is what makes "commit only after the
  * write" expressible as stream topology instead of as a convention.
  */
final case class DecodedRecord(
  record: ConsumerRecord[String, Array[Byte]],
  committable: Committable,
  outcome: Either[DeadLetter, PendingWrite]
)

/** Turns a committable Kafka message into a [[DecodedRecord]], inside a CONSUMER span.
  *
  * **Total by construction.** Every failure path — an unrecognised content mode, malformed JSON, a missing required
  * attribute, an envelope with no `time` and therefore no partition to land in, and even an unexpected runtime
  * exception — becomes a `Left(DeadLetter)`. Nothing here throws, because the only thing a throw could achieve is to
  * kill a stream whose restart would meet the same record again.
  *
  * **Trace continuation happens here** (ADR §7.2). `KafkaTrace.withConsumerSpan` extracts the W3C `traceparent` the
  * producer injected and opens a CONSUMER span parented to it, so a trace that starts at wolfram's HTTP ingress
  * continues into this consumer rather than starting a second, unrelated root trace. The span is made current for the
  * duration of the decode, which is what puts `trace_id`/`span_id` into the MDC of anything logged from it.
  *
  * **Known limitation, stated rather than hidden:** the span covers the decode, not the batched insert. The insert is a
  * batch operation spanning many traces at once, so it cannot be a child of any one of them; it gets its own span in
  * [[BatchProcessor]]. Holding every record's span open across the batch boundary was the alternative and it makes the
  * consumer hold one live span per in-flight record — up to `batchSize` of them — with no way to end them if the stream
  * is torn down mid-batch.
  */
final class RecordDecoder(source: Source, tracer: Tracer):

  def decode(message: CommittableMessage[String, Array[Byte]]): DecodedRecord =
    val record = message.record
    val outcome = KafkaTrace.withConsumerSpan(record, tracer)(_ => LogContext.withCurrentSpan(decodeRecord(record)))
    DecodedRecord(record, message.committableOffset, outcome)

  /** The pure part: no span, no offset, no Pekko. Kept separate so its totality is testable on its own. */
  def decodeRecord(record: ConsumerRecord[String, Array[Byte]]): Either[DeadLetter, PendingWrite] =
    try
      KafkaCodecs.decode(record) match
        case Left(failure)   => Left(DeadLetter.of(record, failure, source))
        case Right(envelope) =>
          NewEvent
            .from(envelope)
            .fold(
              // Spec-valid but unstorable: CloudEvents makes `time` optional, the partition key does not. Inventing a
              // timestamp would file the event in a month it did not happen in, so it is dead-lettered instead.
              reason => Left(DeadLetter.of(record, DecodeFailure.Unconvertible(reason), source)),
              event => Right(PendingWrite(record, envelope, event))
            )
    catch
      case NonFatal(error) =>
        Left(DeadLetter.of(record, DecodeFailure.Unconvertible(RecordDecoder.describe(error)), source))

object RecordDecoder:

  /** Kafka and JSON exceptions frequently carry a null message, and a `null` in a DLQ `detail` field is worse than the
    * class name.
    */
  def describe(error: Throwable): String =
    Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getName)
