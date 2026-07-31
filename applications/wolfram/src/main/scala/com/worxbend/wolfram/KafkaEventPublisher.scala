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

package com.worxbend.wolfram

import com.typesafe.scalalogging.StrictLogging
import com.worxbend.eventing.KafkaCodecs
import com.worxbend.eventing.KafkaTrace
import com.worxbend.kernel.event.Envelope
import com.worxbend.observability.LogContext
import com.worxbend.observability.Meters
import com.worxbend.observability.Tracing
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.context.Context
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Publishes validated envelopes to `events.cloudevents.v1` with a plain `KafkaProducer`.
  *
  * **No Pekko here, by ADR §1.** wolfram is a request/response service: every produce belongs to exactly one in-flight
  * HTTP request, so there is no stream to backpressure and nothing for a materializer to do. A `Producer` plus a
  * `Promise` is the whole of it, and it keeps Pekko off this service's classpath entirely.
  *
  * **`send` is not as asynchronous as it looks, and this class is built around that.** `KafkaProducer.send` blocks the
  * calling thread while topic metadata is unknown or the record accumulator is full, for up to `max.block.ms`. On a
  * Vert.x event loop that is not a latency problem, it is an availability one — the loop serves every other connection
  * too. So sends are handed to a **single-threaded executor with a bounded queue**:
  *
  *   - *single-threaded* because the accumulator is what actually batches, and one thread calling `send` in order keeps
  *     the per-key ordering that `Envelope.partitionKey` exists to provide. A pool would let two records for the same
  *     device race into the accumulator in the wrong order.
  *   - *bounded* because that is the backpressure. When the queue is full the executor's abort policy throws, the
  *     request is refused with a 503, and the client backs off. The alternative — an unbounded queue — converts broker
  *     unavailability into heap exhaustion, and turns a shed request into an OOM kill several minutes later.
  *
  * **Trace context is captured, never inherited.** `Context.current()` is read on the *calling* thread, at the moment
  * `publish` is invoked, and passed explicitly across the hand-off. ADR §7.2 names this as the standing hazard: OTel's
  * context is ThreadLocal-backed, so reading it on the sender thread would silently return root and orphan every
  * produce span from the ingest span that caused it.
  *
  * The span ends in the broker callback, not when `send` returns, so `kafka.produce.latency` and the span duration
  * measure the same thing: acknowledgement, which is what `acks=all` makes meaningful.
  */
final class KafkaEventPublisher(
  producer: Producer[String, Array[Byte]],
  topic: String,
  sender: Executor,
  health: BrokerHealth,
  metrics: IngestMetrics,
  tracing: Tracing,
  closeTimeout: FiniteDuration,
  now: () => Instant = () => Instant.now(),
  onClose: () => Unit = () => ()
) extends EventPublisher
    with StrictLogging:

  def brokerReachable: Boolean = health.reachable

  def publish(envelope: Envelope): Future[Either[Rejection, PublishAck]] =
    KafkaCodecs.producerRecord(topic, envelope) match
      case Left(reason) =>
        // Encoding an envelope this service already validated should be impossible; if it happens it is a bug in
        // validation, not a client error, so it is logged loudly and still answered as a 400 rather than a 500 —
        // the client's event is genuinely unrepresentable on this wire.
        logger.error(s"validated envelope could not be encoded for Kafka: $reason")
        Future.successful(Left(Rejection.InvalidAttributes(reason)))
      case Right(record) =>
        val context = Context.current()
        val promise = Promise[Either[Rejection, PublishAck]]()
        try
          sender.execute(() => dispatch(record, context, promise))
          promise.future
        catch
          case _: RejectedExecutionException =>
            // The queue is full: the broker is not keeping up with ingestion. Shedding here is the point of the
            // bound — see the class Scaladoc.
            health.observe(reachable = false, now(), Some("publish queue full"))
            Future.successful(
              Left(
                Rejection.BrokerUnavailable(
                  "the publish queue is full; the broker is not acknowledging records fast enough"
                )
              )
            )

  /** Runs on the sender thread. Everything here may block for up to `max.block.ms`. */
  private def dispatch(
    record: ProducerRecord[String, Array[Byte]],
    context: Context,
    promise: Promise[Either[Rejection, PublishAck]]
  ): Unit =
    val started = System.nanoTime()
    val span = tracing.tracer
      .spanBuilder(KafkaEventPublisher.SpanName)
      .setSpanKind(SpanKind.PRODUCER)
      .setParent(context)
      .setAllAttributes(KafkaEventPublisher.attributesOf(topic, record.key))
      .startSpan()
    val scope = span.makeCurrent()
    try
      // The carrier is `modules/eventing`'s, so the header names and the propagator match byte-for-byte what cobalt
      // extracts with — the whole cross-service trace hangs on these two using one implementation (ADR §7.2).
      val _ = KafkaTrace.inject(record, Context.current())
      val _ = producer.send(
        record,
        (metadata, error) =>
          val elapsed = System.nanoTime() - started
          if error == null then
            span.end()
            metrics.published(topic, Meters.Outcomes.Success, elapsed)
            health.observe(reachable = true, now())
            val _ = promise.success(
              Right(PublishAck(metadata.topic, metadata.partition, metadata.offset, record.key))
            )
          else
            val cause = KafkaEventPublisher.describe(error)
            val _ = span.setStatus(StatusCode.ERROR, cause)
            val _ = span.recordException(error)
            span.end()
            metrics.published(topic, Meters.Outcomes.Failure, elapsed)
            health.observe(reachable = false, now(), Some(cause))
            LogContext.withSpan(span):
              logger.warn(s"broker refused a record for topic $topic: $cause", error)
            val _ = promise.success(Left(Rejection.BrokerUnavailable(cause)))
      )
    catch
      // send() throws synchronously when metadata cannot be fetched within max.block.ms, when the buffer is
      // exhausted, or when serialization fails. All three are "not durable", so they answer like a broker refusal.
      case NonFatal(error) =>
        val cause = KafkaEventPublisher.describe(error)
        val _ = span.setStatus(StatusCode.ERROR, cause)
        val _ = span.recordException(error)
        span.end()
        metrics.published(topic, Meters.Outcomes.Failure, System.nanoTime() - started)
        health.observe(reachable = false, now(), Some(cause))
        logger.warn(s"could not hand a record to the broker for topic $topic: $cause", error)
        val _ = promise.success(Left(Rejection.BrokerUnavailable(cause)))
    finally scope.close()

  /** Asks the broker for topic metadata and records the answer.
    *
    * This is what keeps readiness honest while the service is idle: without it, a broker that died after the last
    * request would be believed reachable until the next one arrived — i.e. readiness would only ever tell the truth
    * about the past.
    */
  def probe(): Unit =
    try
      val partitions = producer.partitionsFor(topic)
      if partitions.isEmpty then
        health.observe(reachable = false, now(), Some(s"topic '$topic' has no partitions"))
      else health.observe(reachable = true, now())
    catch
      case NonFatal(error) =>
        health.observe(reachable = false, now(), Some(KafkaEventPublisher.describe(error)))

  def flush(): Unit = producer.flush()

  /** Graceful shutdown: stop accepting, drain, then close.
    *
    * The order matters and is the whole reason this is not just `producer.close()`. `onClose` shuts the sender queue
    * down so no new record is enqueued; `flush` blocks until everything already accepted is acknowledged — those are
    * records for which a client has *already been given a 202*, and dropping them would make the API a liar; only then
    * is the producer closed, with a bounded timeout so an unreachable broker cannot hold a rolling deploy open
    * indefinitely.
    */
  def close(): Unit =
    onClose()
    try producer.flush()
    catch case NonFatal(error) => logger.warn("flushing the producer during shutdown failed", error)
    producer.close(java.time.Duration.ofMillis(closeTimeout.toMillis))

object KafkaEventPublisher:

  /** Span name for the produce boundary — one of the four manual boundaries of ADR §7.2. */
  val SpanName: String = "kafka.produce"

  private val MessagingSystem: AttributeKey[String] = AttributeKey.stringKey("messaging.system")
  private val MessagingDestination: AttributeKey[String] = AttributeKey.stringKey("messaging.destination.name")
  private val MessagingKey: AttributeKey[String] = AttributeKey.stringKey("messaging.kafka.message.key")

  /** OTel semantic-convention attributes for a Kafka produce. Spelled out rather than taken from
    * `opentelemetry-semconv`'s incubating classes, which rename constants between minor versions.
    */
  def attributesOf(topic: String, key: String): Attributes =
    Attributes
      .builder()
      .put(MessagingSystem, "kafka")
      .put(MessagingDestination, topic)
      .put(MessagingKey, key)
      .build()

  /** A Kafka exception's message is frequently null (`TimeoutException` in particular), and a null in an HTTP body or a
    * span status is worse than the class name.
    */
  def describe(error: Throwable): String =
    Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getName)

  /** Builds the production publisher: a real `KafkaProducer`, the bounded sender, and the readiness prober.
    *
    * The serializers are `String`/`Array[Byte]` and **not** `io.cloudevents.kafka.CloudEventSerializer`: the record is
    * already fully formed by `KafkaCodecs.producerRecord`, which writes the binary-mode headers itself because the
    * SDK's serializer renders `time` in a shape that is not RFC 3339 (see `modules/eventing`'s `ContentMode`). Letting
    * the SDK re-encode here would reintroduce exactly the defect that module exists to avoid.
    */
  def start(
    config: PublisherConfig,
    metrics: IngestMetrics,
    tracing: Tracing,
    health: BrokerHealth = BrokerHealth()
  ): KafkaEventPublisher =
    val settings = config.properties ++ Map(
      "bootstrap.servers" -> config.bootstrapServers,
      "max.block.ms" -> config.maxBlock.toMillis.toString,
      "delivery.timeout.ms" -> config.deliveryTimeout.toMillis.toString,
      "request.timeout.ms" -> config.requestTimeout.toMillis.toString
    )
    val producer =
      KafkaProducer[String, Array[Byte]](
        KafkaCodecs.producerConfig(settings),
        StringSerializer(),
        ByteArraySerializer()
      )
    val sender = boundedSender(config.queueCapacity)
    val prober: ScheduledExecutorService =
      Executors.newSingleThreadScheduledExecutor(daemon("wolfram-kafka-probe"))

    val stopExecutors: () => Unit = () =>
      shutdown(prober)
      shutdown(sender)

    val publisher =
      new KafkaEventPublisher(
        producer = producer,
        topic = config.topic,
        sender = sender,
        health = health,
        metrics = metrics,
        tracing = tracing,
        closeTimeout = config.closeTimeout,
        onClose = stopExecutors
      )

    // Probed once synchronously so the first readiness answer is evidence rather than pessimism, then on a schedule so
    // an idle service still notices a broker that went away.
    publisher.probe()
    val _ = prober.scheduleWithFixedDelay(
      () => publisher.probe(),
      config.maxBlock.toMillis,
      ProbeIntervalMillis,
      TimeUnit.MILLISECONDS
    )
    publisher

  /** How often readiness re-checks an idle broker. Shorter than a typical readiness period so the answer a probe reads
    * is never more than one interval stale.
    */
  val ProbeIntervalMillis: Long = 5000L

  /** One thread, a bounded queue, and an abort policy — see the class Scaladoc for why each of the three. */
  private def boundedSender(queueCapacity: Int): ThreadPoolExecutor =
    ThreadPoolExecutor(
      1,
      1,
      0L,
      TimeUnit.MILLISECONDS,
      ArrayBlockingQueue[Runnable](queueCapacity),
      daemon("wolfram-kafka-sender"),
      ThreadPoolExecutor.AbortPolicy()
    )

  private def daemon(name: String): java.util.concurrent.ThreadFactory =
    runnable =>
      val thread = Thread(runnable, name)
      thread.setDaemon(true)
      thread

  private def shutdown(executor: java.util.concurrent.ExecutorService): Unit =
    executor.shutdown()
    // Whatever is still queued has already been promised to a client, so it is drained rather than discarded; the
    // bound is short because `close` still has the producer's own flush timeout to spend after this.
    if !executor.awaitTermination(5, TimeUnit.SECONDS) then
      val abandoned = executor.shutdownNow().asScala.size
      if abandoned > 0 then
        // Deliberately not logged through the class logger: this is the companion, and a shutdown that reaches here
        // has already lost records, which is a fact the operator needs on stderr even if logging is torn down.
        System.err.println(s"wolfram: abandoned $abandoned queued Kafka sends during shutdown")
