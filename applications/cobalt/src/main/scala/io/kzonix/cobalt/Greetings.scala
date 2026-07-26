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

import pureconfig.ConfigReader
import pureconfig.ConfigSource

/** Server binding, loaded with pureconfig and overridable from the environment. */
final case class ServerConfig(host: String, port: Int) derives ConfigReader

object ServerConfig:
  def load(): ServerConfig = ConfigSource.default.at("server").loadOrThrow[ServerConfig]

/** Response payloads and their rendering.
  *
  * Kept separate from the route annotations so the behaviour can be tested without binding a socket.
  */
object Greetings:

  def health: ujson.Obj = ujson.Obj("status" -> "UP")

  def greet(name: String): ujson.Obj =
    ujson.Obj("message" -> s"Hello, ${name.trim}")

  /** Names arrive from the path, so an empty or whitespace-only segment is rejected rather than rendered. */
  def validateName(name: String): Either[String, String] =
    if name.trim.isEmpty then Left("name must not be blank") else Right(name.trim)
