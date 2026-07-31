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
import com.worxbend.observability.Telemetry
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServer
import io.vertx.ext.web.Router
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal
import sttp.tapir.server.tracing.opentelemetry.OpenTelemetryTracing
import sttp.tapir.server.vertx.VertxFutureServerInterpreter
import sttp.tapir.server.vertx.VertxFutureServerOptions

/** The composition root: every dependency is constructed here, once, and passed down.
  *
  * **Explicit and not a framework.** wolfram has no Guice module and no global registry because it has exactly one
  * object graph and that graph is eleven lines long; a container would hide the two things that actually matter about
  * it — the construction order (telemetry before anything that meters, the publisher before the service that uses it)
  * and the *reverse* order in which it must be torn down.
  *
  * **Shutdown order is the interesting part.** Stop the HTTP server first, so no new request can be accepted; then
  * close the publisher, which drains records that clients have *already been told* were accepted; only then close
  * telemetry, so the spans and metrics describing that drain are still exportable while it happens; Vert.x last,
  * because everything above runs on its threads. Doing telemetry first is the common mistake and it makes the shutdown
  * path the one part of the system with no observability.
  */
final class WolframApp private (
  vertx: Vertx,
  server: HttpServer,
  telemetry: Telemetry,
  publisher: EventPublisher
) extends AutoCloseable
    with StrictLogging:

  /** The port actually bound. Differs from the configured one when the config asks for `0`, which is how a test binds
    * an ephemeral port.
    */
  def port: Int = server.actualPort

  def close(): Unit =
    quietly("closing the HTTP server")(await(server.close()))
    quietly("closing the Kafka publisher")(publisher.close())
    quietly("closing telemetry")(telemetry.close())
    quietly("closing Vert.x")(await(vertx.close()))

  private def quietly(what: String)(action: => Unit): Unit =
    try action
    catch case NonFatal(error) => logger.warn(s"$what failed during shutdown", error)

  private def await(future: io.vertx.core.Future[Void]): Unit =
    val _ = future.toCompletionStage.toCompletableFuture.get(WolframApp.ShutdownTimeoutSeconds, TimeUnit.SECONDS)

object WolframApp:

  /** Bound on each shutdown step, so an unresponsive dependency cannot hold a rolling deploy open. */
  val ShutdownTimeoutSeconds: Long = 10L

  /** How long to wait for the listener to bind before declaring the boot failed. */
  val BindTimeout: Duration = 30.seconds

  /** Builds and starts everything.
    *
    * Blocks until the listener is bound: a `main` that returns before the port is open reports success for a process
    * that may be about to exit with "address already in use", and a test that races the bind is a flaky test.
    */
  def start(config: WolframConfig, telemetry: Telemetry): WolframApp =
    // Vert.x's own event loop must not run application logic: the Kafka publisher hands blocking work to its own
    // thread, but response encoding and future callbacks still need somewhere to run, and borrowing the loop for them
    // would couple every connection's latency to every other's.
    given ExecutionContext = ExecutionContext.global

    val vertx = Vertx.vertx()
    val metrics = IngestMetrics(telemetry.registry)
    val publisher = KafkaEventPublisher.start(config.publisher, metrics, telemetry.tracing)
    val service = IngestionService(publisher, TimeClamp.from(config.ingest), config.ingest, metrics)
    val api = IngestApi(service)

    // The tracing interceptor is prepended so the SERVER span is open before anything else — including the metrics
    // interceptor — runs, which is what puts `trace_id` in the MDC of every log line the request produces (ADR §7.2).
    val options: VertxFutureServerOptions =
      VertxFutureServerOptions.customiseInterceptors
        .prependInterceptor(OpenTelemetryTracing[Future](telemetry.tracing.openTelemetry))
        .metricsInterceptor(HttpMetrics.interceptor(telemetry.registry))
        .options

    val interpreter = VertxFutureServerInterpreter(options)
    val router = Router.router(vertx)
    api.routes.foreach: route =>
      val _ = interpreter.route(route)(router)
    AdminRoutes(telemetry, publisher).mount(router)

    val bound = vertx
      .createHttpServer()
      .requestHandler(router)
      .listen(config.server.port, config.server.host)
      .toCompletionStage
      .toCompletableFuture
      .get(BindTimeout.toSeconds, TimeUnit.SECONDS)

    new WolframApp(vertx, bound, telemetry, publisher)

/** The process entry point. */
object Main extends StrictLogging:

  def main(args: Array[String]): Unit =
    val _ = args
    val config = WolframConfig
      .load()
      .fold(
        failures => throw IllegalStateException(s"wolfram configuration is unusable: ${failures.prettyPrint()}"),
        identity
      )
    val telemetry = Telemetry.start(ServiceName)
    val app = WolframApp.start(config, telemetry)
    logger.info(s"wolfram listening on ${config.server.host}:${app.port}, publishing to ${config.publisher.topic}")

    // A shutdown hook rather than signal handling: SIGTERM from an orchestrator is the only way this process is
    // expected to stop, and the hook is the one mechanism that also covers `System.exit` from a failed subsystem.
    Runtime.getRuntime.addShutdownHook(Thread(() => app.close(), "wolfram-shutdown"))

  /** The `service` tag on every meter and the `service.name` resource attribute on every span. */
  val ServiceName: String = "wolfram"
