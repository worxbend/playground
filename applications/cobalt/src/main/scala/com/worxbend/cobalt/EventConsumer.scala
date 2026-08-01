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

package com.worxbend.cobalt

import com.typesafe.scalalogging.StrictLogging
import java.util.concurrent.atomic.AtomicReference
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.kafka.CommitterSettings
import org.apache.pekko.kafka.ConsumerSettings
import org.apache.pekko.kafka.Subscriptions
import org.apache.pekko.kafka.scaladsl.Committer
import org.apache.pekko.kafka.scaladsl.Consumer
import org.apache.pekko.stream.scaladsl.Sink
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** The running consumer: the graph, its restart supervision, and its shutdown contract.
  *
  * **Shutdown is the reason this is a class and not four lines in `Main`.** A SIGTERM arrives while a batch of up to
  * `batchSize` records is mid-insert. Killing the stream there loses nothing durable — the offsets were not committed —
  * but it *does* mean those records are re-consumed and re-inserted on the next boot, and it means every rolling deploy
  * replays a batch. `drainAndShutdown` instead stops the Kafka fetcher, lets the in-flight batch finish and commit, and
  * only then completes. Wired to `CoordinatedShutdown`'s `PhaseServiceRequestsDone`, that is what makes a rolling
  * deploy invisible in the data.
  *
  * The `Consumer.Control` lives behind an `AtomicReference` that [[ConsumerStream.restarting]] re-sets on every restart
  * attempt; see that method for the failure mode a single capture produces.
  */
final class EventConsumer private (
  control: AtomicReference[Consumer.Control],
  val streamCompletion: Future[Done]
)(using ec: ExecutionContext)
    extends ConsumerHandle,
      StrictLogging:

  /** The supervisor's view of this stream: one future that completes when it ends, however it ends. */
  def completion: Future[Done] = streamCompletion

  /** Stops fetching, finishes what is in flight, commits it, then shuts the consumer down.
    *
    * Reads the control at call time rather than at construction time — that is the entire point of the reference.
    */
  def drain(): Future[Done] =
    logger.info("draining the Kafka consumer before shutdown")
    Consumer
      .DrainingControl((control.get(), streamCompletion))
      .drainAndShutdown()
      .map: _ =>
        logger.info("the Kafka consumer drained cleanly")
        Done

object EventConsumer extends StrictLogging:

  /** Builds and runs the consumer, and registers its drain with `CoordinatedShutdown`.
    *
    * `PhaseServiceRequestsDone` is the phase that runs before anything tears down the actor system's own machinery, so
    * the stream still has a materializer and a dispatcher while it drains. Registering later (in `PhaseServiceStop`, or
    * worse in a JVM shutdown hook that races the actor system) gives a drain that starts and then loses the threads it
    * needs to finish.
    */
  def start(
    config: ConsumerConfig,
    restart: RestartConfig,
    decoder: RecordDecoder,
    processor: BatchProcessor
  )(using system: ActorSystem): EventConsumer =
    given ExecutionContext = system.dispatcher

    val control = AtomicReference[Consumer.Control](Consumer.NoopControl)
    val processing =
      ConsumerStream.processing(
        decode = decoder.decode,
        processor = processor,
        batchSize = config.batchSize,
        batchWindow = config.batchWindow,
        committer = Committer.flow(committerSettings(config))
      )

    val settings = consumerSettings(config)
    val subscription = Subscriptions.topics(config.topic)
    val attempt = () => ConsumerStream.attempt(settings, subscription, processing)
    val restarted = ConsumerStream.restarting(ConsumerStream.restartSettings(restart), control)(attempt)
    val completion = restarted.runWith(Sink.ignore)

    val consumer = EventConsumer(control, completion)
    val _ = CoordinatedShutdown(system).addTask(
      CoordinatedShutdown.PhaseServiceRequestsDone,
      "drain-kafka-consumer"
    )(() => consumer.drain())
    logger.info(s"consuming ${config.topic} as group ${config.groupId} from ${config.bootstrapServers}")
    consumer

  /** The consumer settings, with the three that are correctness properties applied *after* the free-form overrides.
    *
    *   - `enable.auto.commit=false`: auto-commit acknowledges records the moment they are polled, which is the exact
    *     opposite of this pipeline's contract. Leaving it to configuration would let one property file turn
    *     at-least-once into at-most-once with no other visible symptom.
    *   - `auto.offset.reset=earliest`: a new consumer group must see the retained history rather than skip it. `latest`
    *     is how a rebuilt consumer silently loses everything published before it started.
    *   - `max.poll.records`: bounded to the batch size so one poll cannot hand the stream more than one batch's worth
    *     of work; the connector buffers the rest anyway, and the bound keeps `max.poll.interval.ms` meaningful.
    */
  def consumerSettings(config: ConsumerConfig)(using system: ActorSystem): ConsumerSettings[String, Array[Byte]] =
    ConsumerSettings(system, StringDeserializer(), ByteArrayDeserializer())
      .withProperties(config.properties)
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId(config.groupId)
      .withProperty("enable.auto.commit", "false")
      .withProperty("auto.offset.reset", "earliest")
      .withProperty("max.poll.records", config.batchSize.toString)
      .withStopTimeout(config.drainTimeout)

  /** How offsets are aggregated before they are sent to the broker.
    *
    * Aggregating is safe *because* the committer is downstream of the write: every offset it holds is already durable,
    * so delaying the acknowledgement only widens the replay window after a crash, and never loses anything.
    */
  def committerSettings(config: ConsumerConfig)(using system: ActorSystem): CommitterSettings =
    CommitterSettings(system)
      .withMaxBatch(config.commitMaxBatch)
      .withMaxInterval(config.commitMaxInterval)
      .withParallelism(config.commitParallelism)
