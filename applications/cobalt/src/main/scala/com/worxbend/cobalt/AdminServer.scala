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
import io.undertow.Undertow
import java.net.InetSocketAddress
import scala.jdk.CollectionConverters.*

/** The Cask route table. Six routes, each a one-line delegation to [[AdminHandlers]].
  *
  * The paths are string literals because Cask's annotations are macros and want a constant; `AdminRoutesSuite` asserts
  * each one against the corresponding constant in `modules/observability` or [[AdminRoutes]], so a divergence between
  * this file and the shared vocabulary fails a test rather than silently giving Prometheus a 404.
  *
  * **The replay route is the only `@cask.post` in this build, and the only one that changes anything.** It is a POST
  * because it is not safe and not repeatable-without-effect at the broker (each call appends records to the topic);
  * `dryRun` defaults to `true` so the *default* POST is the one that publishes nothing. Everything the operation needs
  * arrives as query parameters rather than a JSON body: the request has four scalar fields, a body parser would mean
  * either upickle — a second JSON stack alongside circe, which ADR §3.3 keeps off the classpath — or hand-rolled
  * parsing, and a query string is what someone can actually type into `curl` mid-incident.
  */
final class CobaltRoutes(handlers: AdminHandlers) extends cask.Routes:

  @cask.get("/metrics")
  def metrics(): cask.Response[String] = CobaltRoutes.respond(handlers.metrics())

  @cask.get("/health/live")
  def live(): cask.Response[String] = CobaltRoutes.respond(handlers.live())

  @cask.get("/health/ready")
  def ready(): cask.Response[String] = CobaltRoutes.respond(handlers.ready())

  @cask.get("/admin/dlq")
  def dlq(): cask.Response[String] = CobaltRoutes.respond(handlers.dlq())

  @cask.get("/admin/dlq/records")
  def dlqRecords(limit: Int = ReplayRequest.UnspecifiedLimit, reason: String = ""): cask.Response[String] =
    CobaltRoutes.respond(handlers.dlqRecords(limit, reason))

  @cask.post("/admin/dlq/replay")
  def dlqReplay(
    limit: Int = ReplayRequest.UnspecifiedLimit,
    reason: String = "",
    refs: String = "",
    dryRun: Boolean = true
  ): cask.Response[String] =
    CobaltRoutes.respond(handlers.dlqReplay(limit, reason, refs, dryRun))

  // --- the consumer lifecycle ------------------------------------------------------------------------------------
  //
  // The colon in these paths is AIP-136's custom-method form, the same one wolfram uses. Cask matches the whole
  // segment literally, so `/admin/consumer:pause` is one route and cannot collide with a sub-resource.

  @cask.get("/admin/consumer")
  def consumer(): cask.Response[String] = CobaltRoutes.respond(handlers.consumerStatus())

  @cask.post("/admin/consumer:pause")
  def consumerPause(): cask.Response[String] = CobaltRoutes.respond(handlers.consumerPause())

  @cask.post("/admin/consumer:resume")
  def consumerResume(): cask.Response[String] = CobaltRoutes.respond(handlers.consumerResume())

  @cask.post("/admin/consumer:stop")
  def consumerStop(): cask.Response[String] = CobaltRoutes.respond(handlers.consumerStop())

  @cask.post("/admin/consumer:start")
  def consumerStart(): cask.Response[String] = CobaltRoutes.respond(handlers.consumerStart())

  @cask.post("/admin/consumer:restart")
  def consumerRestart(
    target: String = SeekTarget.Committed.name,
    offsets: String = "",
    dryRun: Boolean = true
  ): cask.Response[String] =
    CobaltRoutes.respond(handlers.consumerRestart(target, offsets, dryRun))

  @cask.post("/admin/consumer:clearCheckpoints")
  def consumerClearCheckpoints(): cask.Response[String] =
    CobaltRoutes.respond(handlers.consumerClearCheckpoints())

  initialize()

object CobaltRoutes:

  def respond(reply: AdminReply): cask.Response[String] =
    cask.Response(reply.body, statusCode = reply.status, headers = Seq("content-type" -> reply.contentType))

/** cobalt's HTTP listener: Cask's routing, Undertow's lifecycle, owned explicitly.
  *
  * **Undertow is built here rather than by `cask.main.Main.main`.** Cask's own `main` binds the port, registers a JVM
  * shutdown hook and returns nothing — which costs two things this service needs. First, the bound port is unreachable,
  * so an integration test cannot bind port `0` and then talk to it; it has to guess a free port and race every other
  * suite for it. Second, the listener's shutdown belongs in `CoordinatedShutdown` alongside the consumer drain and the
  * pool close, not in a second, independently-ordered JVM hook that may run before or after them.
  *
  * Cask still does all the routing: [[cask.main.Main.defaultHandler]] is its dispatch trie, and this class only decides
  * where it is bound and when it stops.
  */
final class AdminServer(routes: CobaltRoutes, bindHost: String, bindPort: Int)
    extends cask.main.Main
    with StrictLogging:

  def allRoutes: Seq[cask.main.Routes] = Seq(routes)

  override def host: String = bindHost

  override def port: Int = bindPort

  private var listener: Option[Undertow] = None

  /** Binds and starts. Idempotent only in the sense that a second call is a bug — it would leak the first listener, so
    * it throws rather than quietly rebinding.
    */
  def start(): Unit =
    if listener.isDefined then throw IllegalStateException("the admin server is already started")
    cask.main.Main.silenceJboss()
    val server = Undertow.builder.addHttpListener(bindPort, bindHost).setHandler(defaultHandler).build
    server.start()
    listener = Some(server)
    logger.info(s"cobalt admin endpoints listening on $bindHost:$boundPort")

  /** The port actually bound, which differs from the configured one whenever the configuration asks for `0` — the way a
    * test takes an ephemeral port instead of racing for a fixed one.
    */
  def boundPort: Int =
    listener
      .flatMap(_.getListenerInfo.asScala.headOption)
      .map(_.getAddress)
      .collect { case address: InetSocketAddress => address.getPort }
      .getOrElse(bindPort)

  /** Stops accepting, then releases Cask's request executor.
    *
    * Order matters for the same reason it does in wolfram: stopping the listener first means nothing new arrives while
    * the executor drains, and shutting the executor first would fail the requests already in it.
    */
  def stop(): Unit =
    listener.foreach(_.stop())
    listener = None
    executionContext.shutdown()
