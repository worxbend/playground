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

package io.kzonix.tapir

import com.typesafe.scalalogging.StrictLogging
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import pureconfig.ConfigReader
import pureconfig.ConfigSource
import sttp.tapir.server.vertx.VertxFutureServerInterpreter

final case class ServerConfig(host: String, port: Int) derives ConfigReader

object Main extends StrictLogging:

  def main(args: Array[String]): Unit =
    val config = ConfigSource.default.at("server").loadOrThrow[ServerConfig]
    val vertx  = Vertx.vertx()
    val router = Router.router(vertx)

    val interpreter = VertxFutureServerInterpreter()
    Endpoints.all.foreach(endpoint => interpreter.route(endpoint)(router))

    vertx
      .createHttpServer()
      .requestHandler(router)
      .listen(config.port, config.host)
      .onSuccess(_ => logger.info(s"tapir-service listening on ${config.host}:${config.port}"))
      .onFailure(error => logger.error("Failed to bind HTTP server", error))
    ()
