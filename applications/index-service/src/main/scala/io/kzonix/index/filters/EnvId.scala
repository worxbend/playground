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

import com.typesafe.config.Config
import java.util.UUID
import play.api.ConfigLoader

/** Identifier of the deployment environment this instance runs in. */
final case class EnvId(id: String)

object EnvId:

  /** Falls back to a random UUID when the key is absent or blank.
    *
    * The previous fallback used `Random.nextString`, which emits arbitrary UTF-16 code units — including unpaired
    * surrogates and control characters — and the value ends up in an HTTP response header.
    */
  given ConfigLoader[EnvId] =
    (config: Config, path: String) =>
      val configured = if config.hasPath(path) then Option(config.getString(path)) else None
      EnvId(configured.filter(_.trim.nonEmpty).getOrElse(UUID.randomUUID().toString))
