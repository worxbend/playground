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

package io.kzonix.redprime.tasks

import io.kzonix.redprime.actors.RedditUserOverviewActor
import jakarta.inject.Inject
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.Cancellable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

/** Drives [[RedditUserOverviewActor]] on a fixed schedule. Bound eagerly by `TasksModule`. */
@Singleton
final class RedditUserOverviewTask @Inject() (
    actorSystem: ActorSystem,
    @Named("RedditUserOverviewActor") actorRef: ActorRef
)(using ec: ExecutionContext):

  private val schedule: Cancellable =
    actorSystem.scheduler.scheduleAtFixedRate(
      initialDelay = 0.seconds,
      interval = 15.seconds,
      receiver = actorRef,
      message = RedditUserOverviewActor.Tick
    )

  // Stop the timer with the actor system, so a reload in dev mode does not leave
  // an orphaned schedule ticking against a dead actor.
  actorSystem.registerOnTermination(() => schedule.cancel(): Unit)
