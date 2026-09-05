/*
 * Copyright (c) 2020 Worxbend
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

package com.worxbend.ferrite.controllers

import com.typesafe.scalalogging.LazyLogging
import com.worxbend.ferrite.web.view.Failure
import com.worxbend.ferrite.web.view.Presenter
import play.api.mvc.RequestHeader
import play.api.mvc.Result
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Try
import scala.util.control.NonFatal

/** The one place a read that never came back becomes a response.
  *
  * **Aggregation, not a handler per call site.** A page issues four repository queries and a detail view one more; a
  * `recover` on each is five chances to forget one, and forgetting one is invisible until the database is slow. What
  * escapes instead escapes the action entirely, and Play's own error handler then answers with a full `<html>` 500
  * carrying none of this application's response contract — no `Vary: HX-Request`, so htmx splices a whole document
  * inside `<section id="results">` and a shared proxy may cache that 500 against the un-varied URL. The controller is
  * the boundary where "what this failure looks like" is knowable, so the recovery belongs exactly there and once.
  *
  * `render` is supplied by the caller rather than fixed here because the two controllers genuinely differ: the events
  * page has a fragment representation and the overview does not, and that is a fact about those pages, not about
  * failure. What is *not* left to the caller is the model, the status or the logging — those are one decision.
  */
private[controllers] object Unhandled extends LazyLogging:

  /** Runs `result`, turning any failure of it — thrown or `Future`-borne — into `render(Presenter.unavailable)`.
    *
    * `Try` around the by-name argument as well as `recover` on its `Future`, because an action body can also throw
    * before it ever produces one, and the two paths must not differ in what the caller sees.
    *
    * @param backUrl
    *   where the rendered failure offers to send the reader. The request's own URL: a failed search is worth retrying,
    *   unlike a rejected one, and sending them to an unfiltered list would discard the filter they came with.
    */
  def recovered(request: RequestHeader, backUrl: String)(result: => Future[Result])(render: Failure => Result)(using
    ExecutionContext
  ): Future[Result] =
    Try(result).fold(Future.failed, identity).recover { case NonFatal(cause) =>
      // Logged here and nowhere else, with the cause: the rendered page deliberately carries none of it, and a
      // failure that is neither shown nor logged is an outage nobody can explain afterwards.
      logger.error(s"${request.method} ${request.uri} could not be served", cause)
      render(Presenter.unavailable(backUrl))
    }
