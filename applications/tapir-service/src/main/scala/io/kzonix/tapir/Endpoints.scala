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

import io.circe.Codec
import scala.concurrent.Future
import sttp.tapir.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.server.ServerEndpoint

final case class Health(status: String) derives Codec.AsObject
final case class Greeting(message: String) derives Codec.AsObject
final case class ApiError(error: String) derives Codec.AsObject

/** Endpoint descriptions are values.
  *
  * The same definitions drive the Vert.x server below and could equally drive a client or an OpenAPI document, so the
  * contract is stated once rather than restated per interpreter.
  */
object Endpoints:

  val health: PublicEndpoint[Unit, Unit, Health, Any] =
    endpoint.get
      .in("health")
      .out(jsonBody[Health])
      .description("Liveness probe; consults no dependencies.")

  val greet: PublicEndpoint[String, ApiError, Greeting, Any] =
    endpoint.get
      .in("greet" / path[String]("name"))
      .out(jsonBody[Greeting])
      .errorOut(statusCode(sttp.model.StatusCode.BadRequest).and(jsonBody[ApiError]))
      .description("Greets the named caller.")

  def healthLogic(): Either[Unit, Health] = Right(Health("UP"))

  def greetLogic(name: String): Either[ApiError, Greeting] =
    if name.trim.isEmpty then Left(ApiError("name must not be blank"))
    else Right(Greeting(s"Hello, ${name.trim}"))

  val all: List[ServerEndpoint[Any, Future]] =
    List(
      health.serverLogicPure(_ => healthLogic()),
      greet.serverLogicPure(greetLogic)
    )
