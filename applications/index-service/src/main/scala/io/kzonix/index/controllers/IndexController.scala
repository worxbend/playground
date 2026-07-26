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

package io.kzonix.index.controllers

import com.typesafe.scalalogging.StrictLogging
import jakarta.inject.Inject
import jakarta.inject.Singleton
import play.api.cache.AsyncCacheApi
import play.api.libs.json.Json
import play.api.mvc.AbstractController
import play.api.mvc.Action
import play.api.mvc.AnyContent
import play.api.mvc.ControllerComponents
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

@Singleton
final class IndexController @Inject() (
    cc: ControllerComponents,
    cache: AsyncCacheApi
)(using ec: ExecutionContext)
    extends AbstractController(cc)
    with StrictLogging:

  def index: Action[AnyContent] = Action.async:
    Future.successful(Ok(Json.obj("message" -> "Hello world")))

  /** Cache-backed read.
    *
    * The previous implementation stored `null` here to reproduce a Play caffeine NPE; caching a null is not something a
    * service should do, so the probe was dropped and this reads through the cache normally.
    */
  def cached: Action[AnyContent] = Action.async:
    cache
      .getOrElseUpdate[String]("index.greeting", 5.minutes)(Future.successful("Hello from cache"))
      .map(greeting => Ok(Json.obj("message" -> greeting)))
