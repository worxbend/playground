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

import com.worxbend.ferrite.search.SearchQuery
import com.worxbend.ferrite.search.SearchService
import com.worxbend.ferrite.web.Hx
import com.worxbend.ferrite.web.Hx.varyOnHx
import com.worxbend.ferrite.web.Query
import com.worxbend.ferrite.web.Urls
import com.worxbend.ferrite.web.view.EventsPage
import com.worxbend.ferrite.web.view.Failure
import com.worxbend.ferrite.web.view.FilterBar
import com.worxbend.ferrite.web.view.Presenter
import com.worxbend.kernel.Rfc3339
import com.worxbend.persistence.repository.EventRef
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.time.Clock
import java.time.OffsetDateTime
import java.util.UUID
import play.api.mvc.AbstractController
import play.api.mvc.Action
import play.api.mvc.AnyContent
import play.api.mvc.ControllerComponents
import play.api.mvc.RequestHeader
import play.api.mvc.Result
import play.twirl.api.Html
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Try

/** The event observatory.
  *
  * **One URL, two representations.** `/events?…` answers a browser navigation with the whole document and an htmx swap
  * with just the region that changed. The fragment templates are the same templates the page wraps, so there is no
  * second copy of the table to keep in sync — that single decision (ADR §8.4, implemented in [[Hx]]) is what makes htmx
  * pay for itself here rather than doubling the markup. Every response from this controller carries `Vary: HX-Request`,
  * without exception, including the error ones.
  *
  * **Which fragment** is decided by the request, not by a separate endpoint:
  *
  *   - a fragment request *with* a cursor is paging, and gets rows plus a fresh sentinel to swap in place of the old
  *     one — no facets, no histogram, no count, because none of them changed;
  *   - a fragment request *without* a cursor is a filter change, and gets the whole results region.
  *
  * Giving paging its own path would have been simpler here and worse everywhere else: the "load more" link would then
  * not be a shareable position in the same search.
  */
@Singleton
final class EventsController @Inject() (cc: ControllerComponents, service: SearchService, clock: Clock)(using
  ec: ExecutionContext
) extends AbstractController(cc):

  /** The landing page is the unfiltered search. A redirect rather than a second copy of the list, so there is exactly
    * one canonical URL for "all recent events" to share and to cache.
    */
  def index: Action[AnyContent] = Action(Redirect(Urls.Events))

  /** The list, filtered, paged and faceted. */
  def list: Action[AnyContent] = Action.async: request =>
    SearchQuery.parse(request.rawQueryString) match
      case Left(errors) =>
        // The rejected permalink comes back IN the filter bar, every bad value still in the input that produced it
        // (ADR §6.3). An error page without the bar would leave the user holding a URL they can neither see nor edit.
        val bar = Presenter.filterBar(SearchQuery.lenient(request.rawQueryString), errors)
        Future.successful(failure(Presenter.badQuery(errors), Some(bar), request))
      case Right(query) =>
        val now = OffsetDateTime.now(clock)
        if Hx.isFragment(request) && query.cursor.isDefined then
          service.page(query).map {
            case Left(reason) => failure(Presenter.rejected(reason, Urls.events(query.permalink)), None, request)
            case Right(page)  =>
              val rows = page.rows.map(Presenter.row(_, now))
              val more = page.nextCursor.map(cursor => Urls.events(query.continuation(cursor)))
              Ok(views.html.fragments.rows(rows, more)).varyOnHx
          }
        else
          service.search(query).map {
            case Left(reason)   => failure(Presenter.rejected(reason, Urls.events(query.permalink)), None, request)
            case Right(outcome) =>
              val model = EventsPage(Presenter.filterBar(query, Vector.empty), Presenter.results(outcome, query, now))
              if Hx.isFragment(request) then
                // The push-url header keeps the address bar honest when the swap came from the filter bar: the URL a
                // user copies must be the search they are looking at, not the one they arrived with.
                Ok(views.html.fragments.results(model.results))
                  .withHeaders(Hx.PushUrlHeader -> Urls.events(query.permalink))
                  .varyOnHx
              else Ok(views.html.pages.events(model)(request)).varyOnHx
          }

  /** One event: the decoded observation and the raw CloudEvent side by side.
    *
    * `occurred_at` arrives as the `at` query parameter and is not optional — it is half of the primary key of a
    * partitioned table (ADR §5), and a lookup without it would have to scan every partition. A missing or unparseable
    * `at` is therefore a bad request and not a "search harder".
    */
  def detail(rawEventUid: String): Action[AnyContent] = Action.async: request =>
    val back = backUrl(request)
    reference(rawEventUid, request) match
      case Left(reason) => Future.successful(failure(Presenter.rejected(reason, back), None, request))
      case Right(ref)   =>
        service.detail(ref).map {
          case None        => failure(Presenter.notFound(back), None, request)
          case Some(found) =>
            val model = Presenter.detail(found, OffsetDateTime.now(clock), back)
            if Hx.isFragment(request) then Ok(views.html.fragments.detail(model)).varyOnHx
            else Ok(views.html.pages.detail(model)(request)).varyOnHx
        }

  private def reference(rawEventUid: String, request: RequestHeader): Either[String, EventRef] =
    for
      uid <- Try(UUID.fromString(rawEventUid)).toEither.left.map(_ => s"'$rawEventUid' is not an event id")
      raw <- request.getQueryString(Urls.AtParam).toRight(s"the '${Urls.AtParam}' parameter is required")
      at <- Rfc3339.parse(raw)
    yield EventRef(at, uid)

  /** The list URL a detail page came from: this request's query string minus the key of the event being shown. Keeps
    * the user's search alive across a drill-down instead of dumping them back on an unfiltered list.
    */
  private def backUrl(request: RequestHeader): String =
    Urls.events(Query.render(Query.remove(Query.parse(request.rawQueryString), Urls.AtParam)))

  /** Renders a [[Failure]] at its own status code, as a fragment or a page depending on the request.
    *
    * The status and the rendered page come from the same value, so a 400 can never be served with a body that reads
    * like a 404 — a class of mismatch that is otherwise only caught by someone reading the response by hand.
    */
  private def failure(model: Failure, bar: Option[FilterBar], request: RequestHeader): Result =
    val body: Html =
      if Hx.isFragment(request) then views.html.fragments.failure(model)
      else views.html.pages.failure(model, bar)(request)
    Status(model.status)(body).varyOnHx
