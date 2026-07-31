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

import com.worxbend.ferrite.overview.OverviewRange
import com.worxbend.ferrite.overview.OverviewService
import com.worxbend.ferrite.web.Urls
import com.worxbend.ferrite.web.view.OverviewPresenter
import com.worxbend.ferrite.web.view.Presenter
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.time.Clock
import java.time.OffsetDateTime
import play.api.mvc.AbstractController
import play.api.mvc.Action
import play.api.mvc.AnyContent
import play.api.mvc.ControllerComponents
import scala.concurrent.ExecutionContext

/** The overview: what the system is doing right now, in one screen.
  *
  * **`/` is a page, not a redirect.** It used to be a `302` to `/events`, which answered "find me these events" for
  * someone who had asked "what is happening". Monitoring is a first-class activity in this product and it needs a
  * screen of its own.
  *
  * **It reads the rollup, and that is the entire reason it can exist.** `events.event_rollup_hourly` is one row per
  * `(hour, type, source, severity)`; a thirty-day chart is therefore a few thousand rows rather than a scan of the fact
  * table, and the page costs the same for a month as for an hour. See `OverviewRepository` for what that buys and what
  * it costs — every number here is stale by up to one refresh interval, which the page states rather than hides.
  *
  * **Full documents only.** There is no fragment representation and no `Vary: HX-Request`: nothing on this page is an
  * htmx swap, the range selector is three ordinary links, and every panel entry navigates to `/events`. A dashboard
  * whose controls are links is a dashboard that works with JavaScript disabled, survives the back button, and can be
  * pasted into a runbook.
  */
@Singleton
final class OverviewController @Inject() (cc: ControllerComponents, service: OverviewService, clock: Clock)(using
  ec: ExecutionContext
) extends AbstractController(cc):

  def index: Action[AnyContent] = Action.async: request =>
    val range = OverviewRange.parse(request.getQueryString(OverviewRange.Key))
    service.load(range).map {
      // Unreachable for the three ranges the enum admits; kept so a fourth added without a matching window fails
      // visibly here instead of rendering an empty chart that looks like a quiet system.
      case Left(reason) =>
        val model = Presenter.rejected(reason, Urls.Events)
        Status(model.status)(views.html.pages.failure(model, None)(request))
      case Right(overview) =>
        val page = OverviewPresenter.page(overview, OffsetDateTime.now(clock))
        Ok(views.html.pages.overview(page)(request))
    }
