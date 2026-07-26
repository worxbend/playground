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

package io.kzonix.quickstart

import com.typesafe.config.Config
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.Behaviors

/** Guardian actor: owns application startup and shutdown. */
object Launcher:

  sealed trait ApplicationLifecycle
  final case class ApplicationStart(config: Config) extends ApplicationLifecycle
  final case class ApplicationStop(reason: String)  extends ApplicationLifecycle

  def apply(): Behavior[ApplicationLifecycle] =
    Behaviors.setup: context =>
      context.log.info("Initializing application guardian behaviour")
      launcher(isStarted = false)

  private def launcher(isStarted: Boolean): Behavior[ApplicationLifecycle] =
    Behaviors.receive[ApplicationLifecycle]: (context, message) =>
      message match
        case ApplicationStart(_) if isStarted =>
          context.log.info("Application already started, ignoring message")
          Behaviors.same

        case ApplicationStart(config) =>
          context.log.info("Starting")
          start(config, context)
          context.log.info("Started")
          launcher(isStarted = true)

        case ApplicationStop(reason) =>
          context.log.info(s"Stopping: $reason")
          Behaviors.stopped(() => context.log.info(s"${context.self} stopped"))

  /** Wiring point for the components this sandbox exists to experiment with (connection pools, metrics registry,
    * consumers, scheduled tasks).
    */
  private def start(config: Config, context: ActorContext[ApplicationLifecycle]): Unit =
    context.log.debug(s"Application configuration root: ${config.root().keySet()}")
