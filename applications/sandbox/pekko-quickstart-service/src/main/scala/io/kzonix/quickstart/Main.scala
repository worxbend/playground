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

import com.typesafe.config.ConfigFactory
import io.kzonix.quickstart.Launcher.ApplicationStart
import io.kzonix.quickstart.Launcher.ApplicationStop
import org.apache.pekko.Done
import org.apache.pekko.actor.CoordinatedShutdown
import org.apache.pekko.actor.typed.ActorSystem
import scala.concurrent.Future

object Main:

  private val Name = "quickstart"

  def main(args: Array[String]): Unit =
    // Loaded before the actor system so a broken configuration fails fast.
    val config = ConfigFactory.load()
    val system = ActorSystem(Launcher(), Name, config)

    system ! ApplicationStart(config)

    val shutdown = CoordinatedShutdown(system)

    shutdown.addTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, "application-stop"): () =>
      system ! ApplicationStop("Coordinated shutdown requested")
      Future.successful(Done)
