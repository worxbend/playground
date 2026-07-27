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

import java.util.concurrent.atomic.AtomicReference
import org.apache.pekko.Done
import org.apache.pekko.NotUsed
import org.apache.pekko.kafka.ConsumerMessage.Committable
import org.apache.pekko.kafka.ConsumerMessage.CommittableMessage
import org.apache.pekko.kafka.ConsumerSettings
import org.apache.pekko.kafka.Subscription
import org.apache.pekko.kafka.scaladsl.Consumer
import org.apache.pekko.stream.RestartSettings
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.RestartSource
import org.apache.pekko.stream.scaladsl.Source
import scala.concurrent.duration.FiniteDuration

/** The consumer graph of ADR §4.3, assembled from pieces each of which is testable without a broker.
  *
  * ```text
  * committableSource ─► decode ─► groupedWithin ─► mapAsync(1) write ─► mapConcat ─► Committer.flow ─► Sink.ignore
  * ```
  *
  * **The ordering of the last two stages is the whole at-least-once guarantee, and it is the reason this is a `Flow`
  * and not a fold.** An offset is a *receipt for a durable effect*. `Committer.flow` sits strictly downstream of the
  * write, so an offset physically cannot reach the committer until [[BatchProcessor]] has already made its batch
  * durable — either in PostgreSQL or in the DLQ. Put the committer upstream (or run it in parallel, or commit inside
  * the write stage's `andThen`) and a crash in the window between the two loses every event in flight, silently, with
  * the consumer group reporting zero lag. At-least-once redelivery plus the `(occurred_at, ce_source, ce_id)` unique
  * index then makes the pipeline observationally exactly-once at the database.
  *
  * **`mapAsync(1)` and not a larger parallelism.** Offsets must reach the committer monotonically per partition;
  * `mapAsync(n)` preserves *emission* order but starts n batches concurrently, so two batches from one partition would
  * be writing at the same time and a failure in the older one would already have had its successor's offset committed
  * past it. One in flight is also enough: the batch is 500 rows in a single `executeBatch`, so the database, not the
  * stream, is the bottleneck.
  *
  * **The value deserializer is `ByteArrayDeserializer`, never `CloudEventDeserializer`** (ADR §4.3). Decoding happens
  * in the `decode` stage, where a failure is an ordinary `Left` rather than an exception thrown inside
  * `KafkaConsumer.poll` — which would kill the stream before the connector ever saw the record and make every restart
  * replay the same poison pill forever.
  */
object ConsumerStream:

  /** The processing half of the graph: everything between the Kafka source and the sink.
    *
    * Taking `committer` as a parameter rather than calling `Committer.flow` inline is what makes commit *ordering*
    * testable: a unit test substitutes a flow that records when each offset arrived and asserts it never precedes the
    * write it belongs to. That is the one property of this file worth a test, and it is invisible from the outside
    * otherwise.
    */
  def processing(
    decode: CommittableMessage[String, Array[Byte]] => DecodedRecord,
    processor: BatchProcessor,
    batchSize: Int,
    batchWindow: FiniteDuration,
    committer: Flow[Committable, Done, NotUsed]
  ): Flow[CommittableMessage[String, Array[Byte]], Done, NotUsed] =
    Flow[CommittableMessage[String, Array[Byte]]]
      .map(decode)
      .groupedWithin(batchSize, batchWindow)
      .mapAsync(1)(batch => processor.process(batch.toVector))
      .mapConcat(identity)
      .via(committer)

  /** One consumer attempt: a live Kafka source with [[processing]] attached.
    *
    * Materialises the `Consumer.Control` so the caller can capture it — see [[restarting]] for why that capture has to
    * happen per attempt rather than once.
    */
  def attempt(
    settings: ConsumerSettings[String, Array[Byte]],
    subscription: Subscription,
    processing: Flow[CommittableMessage[String, Array[Byte]], Done, NotUsed]
  ): Source[Done, Consumer.Control] =
    Consumer.committableSource(settings, subscription).via(processing)

  /** Wraps an attempt in bounded exponential backoff, re-capturing the control each time.
    *
    * **`control` is re-set inside `mapMaterializedValue`, on every attempt.** This is the stale-control bug ADR §4.3
    * names explicitly: capture the `Consumer.Control` once, outside the restart, and after the first broker hiccup the
    * reference points at a dead consumer. `CoordinatedShutdown` then calls `drainAndShutdown` on it, the call returns
    * immediately and successfully, and the *live* consumer is killed by the JVM exiting underneath it — dropping
    * exactly the in-flight batch that graceful shutdown existed to save. It fails silently and only under load.
    *
    * `onFailuresWithBackoff` and not `withBackoff`: normal completion of the inner source means the consumer was
    * deliberately stopped (a drain), and restarting it would make shutdown unachievable.
    */
  def restarting(
    settings: RestartSettings,
    control: AtomicReference[Consumer.Control]
  )(attempt: () => Source[Done, Consumer.Control]): Source[Done, NotUsed] =
    val capture: () => Source[Done, Consumer.Control] = () =>
      attempt().mapMaterializedValue: consumer =>
        control.set(consumer)
        consumer
    RestartSource.onFailuresWithBackoff(settings)(capture)

  /** [[RestartConfig]] as Pekko's settings object. */
  def restartSettings(config: RestartConfig): RestartSettings =
    RestartSettings(config.minBackoff, config.maxBackoff, config.randomFactor)
      .withMaxRestarts(config.maxRestarts, config.maxRestartsWithin)
