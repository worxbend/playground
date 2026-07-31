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

import pureconfig.ConfigReader
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures
import scala.concurrent.duration.FiniteDuration

/** Where the HTTP listener binds.
  *
  * Separate from the ingestion and publisher settings because it is the one part of the configuration a platform team
  * owns rather than the service team: container orchestrators inject the port, nobody tunes the time clamp from a
  * sidecar.
  */
final case class ServerConfig(host: String, port: Int)

object ServerConfig:

  /** Written with `forProductN` rather than `derives ConfigReader`, matching `modules/persistence`: derivation needs a
    * wildcard given-import that `-Wunused:all` + `-Werror` flags spuriously (ADR §12.4), and the HOCON keys are then
    * literally in the source beside the fields they fill.
    */
  given reader: ConfigReader[ServerConfig] = ConfigReader.forProduct2("host", "port")(ServerConfig.apply)

/** The validation limits wolfram applies before an event is allowed onto the topic.
  *
  * Every one of these is a *rejection* threshold, never a clamping-into-range one. ADR §4.3 is explicit — "reject;
  * never invent defaults" — because each of these values ends up deciding which Postgres partition the row lands in or
  * how much broker memory one request can pin, and silently repairing a producer's mistake makes the mistake permanent
  * and invisible.
  *
  * @param maxEventBytes
  *   ceiling on a single event's raw body. Kafka's own `message.max.bytes` would reject an oversize record too, but it
  *   would do so *after* the API had already accepted the request, turning a 413 into a 503.
  * @param maxBatchEvents
  *   ceiling on the number of events in one `application/cloudevents-batch+json` document. A batch is published
  *   event-by-event, so an unbounded batch is an unbounded number of in-flight sends from one request.
  * @param maxFutureSkew
  *   how far ahead of the ingest clock a `time` may be. ADR §12.4 fixes this at 24 h: enough for the worst plausible
  *   device clock drift, short enough that a garbage timestamp cannot create a partition years ahead.
  * @param maxPastSkew
  *   how far behind. ADR §12.4 fixes this at 90 d, which is the backfill window the partition-maintenance job keeps
  *   open; older than that and there is no partition to land in, so the row would go to the DEFAULT partition.
  */
final case class IngestConfig(
  maxEventBytes: Long,
  maxBatchEvents: Int,
  maxFutureSkew: FiniteDuration,
  maxPastSkew: FiniteDuration
)

object IngestConfig:

  given reader: ConfigReader[IngestConfig] =
    ConfigReader.forProduct4("max-event-bytes", "max-batch-events", "max-future-skew", "max-past-skew")(
      IngestConfig.apply
    )

/** Everything the Kafka producer needs, with the blocking behaviour named rather than left to Kafka's defaults.
  *
  * `KafkaProducer.send` is only *mostly* asynchronous: it blocks the calling thread while topic metadata is unknown or
  * the accumulator is full, for up to `max.block.ms` — whose default is 60 s. On an event loop that is an outage, so
  * this build sets it low and deliberately, and routes sends through a bounded queue (see
  * [[com.worxbend.wolfram.KafkaEventPublisher]]) so that saturation surfaces as a 503 instead of as a growing pile of
  * parked event-loop threads.
  *
  * @param queueCapacity
  *   depth of the publisher's hand-off queue. This is the service's explicit backpressure setting: when it is full,
  *   ingestion sheds load rather than buffering unboundedly in front of a broker that is not keeping up.
  * @param properties
  *   free-form producer overrides (security, DNS, tuning). Merged *under* `KafkaCodecs.producerDefaults`, so a
  *   deployment cannot switch idempotence or `acks=all` off from a config file.
  */
final case class PublisherConfig(
  bootstrapServers: String,
  topic: String,
  maxBlock: FiniteDuration,
  deliveryTimeout: FiniteDuration,
  requestTimeout: FiniteDuration,
  closeTimeout: FiniteDuration,
  queueCapacity: Int,
  properties: Map[String, String]
)

object PublisherConfig:

  given reader: ConfigReader[PublisherConfig] =
    ConfigReader.forProduct8(
      "bootstrap-servers",
      "topic",
      "max-block",
      "delivery-timeout",
      "request-timeout",
      "close-timeout",
      "queue-capacity",
      "properties"
    )(PublisherConfig.apply)

/** wolfram's whole configuration, read once at boot by the composition root.
  *
  * One aggregate rather than three independent lookups so that a typo in any namespace fails the process at start
  * rather than the first time that particular subsystem is exercised — a service that boots and then 503s on its first
  * request is much harder to diagnose than one that refuses to boot.
  */
final case class WolframConfig(server: ServerConfig, ingest: IngestConfig, publisher: PublisherConfig)

object WolframConfig:

  /** The config namespace. */
  val Namespace: String = "wolfram"

  given reader: ConfigReader[WolframConfig] =
    ConfigReader.forProduct3("server", "ingest", "publisher")(WolframConfig.apply)

  /** Loads from the ambient config, returning failures rather than throwing.
    *
    * The decision to abort is the composition root's ([[Main]] does abort); this function stays usable from a test that
    * wants to assert on a bad configuration without killing the JVM.
    */
  def load(source: ConfigSource = ConfigSource.default): Either[ConfigReaderFailures, WolframConfig] =
    source.at(Namespace).load[WolframConfig]
