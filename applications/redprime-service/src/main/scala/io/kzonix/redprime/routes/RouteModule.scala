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

package io.kzonix.redprime.routes

import com.google.inject.AbstractModule
import io.kzonix.sird.ProvidedRouter
import net.codingwell.scalaguice.ScalaModule
import net.codingwell.scalaguice.ScalaMultibinder

/** Contributes this service's routers.
  *
  * Routers only — the Reddit WS client is bound by `ClientModule`. It previously lived here, which made the client's
  * lifecycle invisible to anyone reading `application.conf`.
  */
final class RouteModule extends AbstractModule with ScalaModule:

  override def configure(): Unit =
    val _ = ScalaMultibinder.newSetBinder[ProvidedRouter](binder).addBinding.to[BotContextRoute]
