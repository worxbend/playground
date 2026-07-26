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

package io.kzonix.play.underpressure

import com.google.inject.AbstractModule
import io.kzonix.sird.ProvidedRouter
import net.codingwell.scalaguice.ScalaModule
import net.codingwell.scalaguice.ScalaMultibinder

/** Exposes the health probes and opens the `Set[HealthProvider]` multibinder.
  *
  * Creating the provider set here means a service with no dependency checks still answers `/health/ready` (with an
  * empty check list) rather than failing injector creation.
  */
final class HealthModule extends AbstractModule with ScalaModule:

  override def configure(): Unit =
    val _ = ScalaMultibinder.newSetBinder[HealthProvider](binder)
    val _ = ScalaMultibinder.newSetBinder[ProvidedRouter](binder).addBinding.to[HealthRoute]
