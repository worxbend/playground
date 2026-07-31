/*
 * Copyright (c) 2020 Worxbend
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

package com.worxbend.eventing

import com.worxbend.kernel.event.Envelope
import com.worxbend.kernel.event.Topics
import java.util as ju
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import scala.jdk.CollectionConverters.*

/** Producer and consumer wiring: the only place in the build that turns an [[com.worxbend.kernel.event.Envelope]] into
  * a `ProducerRecord`, or a `ConsumerRecord` back into an envelope.
  *
  * **The asymmetry between the two directions is the point.**
  *
  * Producing may throw. `Serializer.serialize` runs on the caller's thread inside `KafkaProducer.send`, the exception
  * surfaces as a failed `Future` at the call site that built the event, and the event is nobody else's problem yet.
  * Failing loudly there is right: an envelope this build cannot encode is a bug in wolfram's validation, and dropping
  * it quietly would lose an event a client was told had been accepted.
  *
  * Consuming may **never** throw. ADR §4.3 gives the failure in full: a throwing deserializer throws inside
  * `KafkaConsumer.poll`, before the Pekko connector ever sees the record, so the stream dies with the offset
  * uncommitted and the restart replays the same record forever. Every decode here therefore yields
  * `Either[DecodeFailure, Envelope]`, and a poison record becomes a [[DeadLetter]] whose offset is still committed.
  *
  * That is also why `io.cloudevents.kafka.CloudEventDeserializer` is not used and must not be introduced: it throws.
  * [[envelopeDeserializer]] is safe in the same slot precisely because its value type is an `Either` — the hazard was
  * never "deserializing in the consumer", it was "throwing in the consumer".
  */
object KafkaCodecs:

  /** What a consumer gets for one record: a decoded envelope, or the reason it could not be decoded. */
  type Decoded = Either[DecodeFailure, Envelope]

  /** Builds the record wolfram publishes.
    *
    * The key defaults to `Envelope.partitionKey` — kernel's single definition — so per-device ordering cannot drift
    * between the producer and any future republisher. `mode` defaults to binary, the mode of `events.cloudevents.v1`.
    *
    * Partition and timestamp are left for the broker to assign. A producer-side timestamp would be the *ingest* clock,
    * and this system already carries the producer's own `time` attribute, which is the one that decides which partition
    * an event is stored in downstream (ADR §5).
    */
  def producerRecord(
    topic: String,
    envelope: Envelope,
    mode: ContentMode = ContentMode.Binary,
    key: Option[String] = None
  ): Either[String, ProducerRecord[String, Array[Byte]]] =
    val headers: Headers = RecordHeaders()
    ContentMode.write(mode, envelope, headers).map: value =>
      val noPartition: java.lang.Integer = null
      val noTimestamp: java.lang.Long = null
      ProducerRecord(topic, noPartition, noTimestamp, key.getOrElse(envelope.partitionKey), value.orNull, headers)

  /** Builds the DLQ record for a record that could not be processed.
    *
    * Structured mode and keyed on `(topic, partition, offset)` via `Topics.dlqKey`, both per ADR §4.3: the key means a
    * replayed poison record overwrites its predecessor under compaction instead of accumulating a copy per retry.
    */
  def deadLetterRecord(
    deadLetter: DeadLetter,
    topic: String = Topics.CloudEventsDlq
  ): Either[String, ProducerRecord[String, Array[Byte]]] =
    producerRecord(topic, deadLetter.toEnvelope, ContentMode.Structured, Some(deadLetter.origin.dlqKey))

  /** Decodes a consumed record. Total — see the object Scaladoc for why that is not negotiable. */
  def decode(record: ConsumerRecord[String, Array[Byte]]): Decoded =
    ContentMode.read(record.headers, Option(record.value))

  /** Decodes a consumed record, turning a failure into everything needed to dead-letter and later replay it. */
  def decodeOrDeadLetter(record: ConsumerRecord[String, Array[Byte]]): Either[DeadLetter, Envelope] =
    decode(record).left.map(failure => DeadLetter.of(record, failure))

  /** A `Serializer` for the producer's value slot, so wolfram can hold a `KafkaProducer[String, Envelope]` and never
    * name an SDK type.
    *
    * Only the headers-aware overload can work — in binary mode the attributes *are* the headers — so the two-argument
    * `serialize(topic, data)` that Kafka keeps for source compatibility fails loudly rather than emitting a record that
    * would come back as `UnknownEncoding`. Every Kafka client since 2.1 calls the three-argument form.
    */
  def envelopeSerializer(mode: ContentMode = ContentMode.Binary): Serializer[Envelope] =
    new Serializer[Envelope]:
      def serialize(topic: String, data: Envelope): Array[Byte] =
        val _ = data
        throw SerializationException(
          s"CloudEvents serialization for topic '$topic' needs the record headers; use a client that calls " +
            "serialize(topic, headers, data)"
        )

      override def serialize(topic: String, headers: Headers, data: Envelope): Array[Byte] =
        val _ = topic
        if data == null then null
        else
          ContentMode.write(mode, data, headers) match
            case Right(value) => value.orNull
            case Left(reason) => throw SerializationException(reason)

  /** A `Deserializer` whose value type is an `Either`, and which therefore cannot throw inside `poll`.
    *
    * Offered as an alternative to decoding in a stream stage, not a replacement: a Pekko Streams consumer still
    * benefits from decoding downstream, where back-pressure and per-record metrics live. What this adds is the option
    * of a plain `KafkaConsumer` — an admin tool, a replayer, an integration test — that is poison-proof by
    * construction.
    */
  def envelopeDeserializer: Deserializer[Decoded] =
    new Deserializer[Decoded]:
      def deserialize(topic: String, data: Array[Byte]): Decoded =
        val _ = data
        Left(
          DecodeFailure.UnknownEncoding(
            s"CloudEvents deserialization for topic '$topic' needs the record headers; this client did not supply them"
          )
        )

      override def deserialize(topic: String, headers: Headers, data: Array[Byte]): Decoded =
        val _ = topic
        ContentMode.read(headers, Option(data))

  /** The producer settings ADR §4.3 requires.
    *
    * They live here rather than in wolfram's configuration because each is a correctness property of *this* wire
    * contract, not a tuning knob: idempotence plus `acks=all` is what makes the `(source, id)` deduplication in
    * Postgres meaningful, and a service quietly running with `acks=1` would lose events the API had already
    * acknowledged.
    */
  val producerDefaults: Map[String, String] =
    Map(
      "enable.idempotence" -> "true",
      "acks" -> "all",
      "compression.type" -> "zstd",
      "linger.ms" -> "5"
    )

  /** [[producerDefaults]] merged over `overrides`, as the `java.util.Map` `KafkaProducer`'s constructor wants.
    *
    * `overrides` wins for everything except the four keys above, which are reapplied afterwards: a deployment may set
    * bootstrap servers and security settings freely, but must not be able to turn idempotence off from a config file.
    */
  def producerConfig(overrides: Map[String, String]): ju.Map[String, AnyRef] =
    (overrides ++ producerDefaults).view.mapValues(v => v: AnyRef).toMap.asJava
