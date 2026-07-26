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

package io.kzonix.redprime.actors

import com.typesafe.scalalogging.StrictLogging
import io.kzonix.redprime.actors.RedditUserOverviewActor.Tick
import io.kzonix.redprime.client.RedditClient
import jakarta.inject.Inject
import org.apache.pekko.actor.Actor
import scala.concurrent.ExecutionContext
import scala.util.Failure
import scala.util.Success

object RedditUserOverviewActor:
  case object Tick

/** Refreshes the Reddit session on a schedule.
  *
  * A classic (untyped) Pekko actor, because Play's `PekkoGuiceSupport.bindActor` binds `pekko.actor.Actor` subclasses.
  */
final class RedditUserOverviewActor @Inject() (redditClient: RedditClient)(using ec: ExecutionContext)
    extends Actor
    with StrictLogging:

  override def receive: Receive =
    case Tick =>
      // The failure branch matters: an unhandled failed Future here would be
      // silently dropped, leaving a broken login indistinguishable from success.
      redditClient
        .login()
        .onComplete:
          case Success(Some(oauth)) => logger.info(s"Reddit session refreshed, expires in ${oauth.expiresIn}s")
          case Success(None)        => logger.warn("Could not obtain access to the Reddit account")
          case Failure(error)       => logger.error("Reddit login failed", error)
