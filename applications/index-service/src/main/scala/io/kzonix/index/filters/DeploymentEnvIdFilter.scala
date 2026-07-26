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

package io.kzonix.index.filters

import jakarta.inject.Inject
import jakarta.inject.Singleton
import play.api.Configuration
import play.api.mvc.EssentialAction
import play.api.mvc.EssentialFilter
import scala.concurrent.ExecutionContext

/** Stamps every response with the deployment environment id, so a response can be traced back to the instance that
  * produced it.
  */
@Singleton
final class DeploymentEnvIdFilter @Inject() (config: Configuration)(using ec: ExecutionContext) extends EssentialFilter:

  // Resolved once: the value is static for the lifetime of the process, and the
  // previous per-request lookup re-parsed configuration on every call.
  private val envId: String = config.getOptional[EnvId]("docker.env").fold("")(_.id)

  override def apply(next: EssentialAction): EssentialAction =
    EssentialAction: request =>
      next(request).map(_.withHeaders("X-Container-Id" -> envId))
