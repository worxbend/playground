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

package io.kzonix.cask

import com.typesafe.scalalogging.StrictLogging

/** Cask entry point: routes are annotations, and the handler bodies delegate to [[Greetings]] so the logic stays
  * testable without starting a server.
  */
object CaskService extends cask.MainRoutes with StrictLogging:

  private val config = ServerConfig.load()

  override def host: String = config.host
  override def port: Int = config.port

  @cask.get("/health")
  def health(): String =
    Greetings.health.render()

  @cask.get("/greet/:name")
  def greet(name: String): cask.Response[String] =
    Greetings.validateName(name) match
      case Right(valid) => cask.Response(Greetings.greet(valid).render(), statusCode = 200)
      case Left(reason) => cask.Response(ujson.Obj("error" -> reason).render(), statusCode = 400)

  logger.info(s"cask-service listening on $host:$port")

  initialize()
