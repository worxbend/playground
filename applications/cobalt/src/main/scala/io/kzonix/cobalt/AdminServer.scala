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

import com.typesafe.scalalogging.StrictLogging
import io.undertow.Undertow
import java.net.InetSocketAddress
import scala.jdk.CollectionConverters.*

/** The Cask route table. Three routes, each a one-line delegation to [[AdminHandlers]].
  *
  * The paths are string literals because Cask's annotations are macros and want a constant; `AdminServerSuite` asserts
  * each one against the corresponding constant in `modules/observability`, so a divergence between this file and the
  * shared vocabulary fails a test rather than silently giving Prometheus a 404.
  */
final class CobaltRoutes(handlers: AdminHandlers) extends cask.Routes:

  @cask.get("/metrics")
  def metrics(): cask.Response[String] = CobaltRoutes.respond(handlers.metrics())

  @cask.get("/health/live")
  def live(): cask.Response[String] = CobaltRoutes.respond(handlers.live())

  @cask.get("/health/ready")
  def ready(): cask.Response[String] = CobaltRoutes.respond(handlers.ready())

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
