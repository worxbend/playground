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

package io.kzonix.ferrite.web

import play.api.mvc.RequestHeader
import play.api.mvc.Result

/** The single decision that makes htmx work without duplicating markup.
  *
  * **A fragment never knows how it is being served; the controller decides** (ADR §8.4). One URL — `/events?…` — has
  * two representations: the whole document for a browser navigation, and the bare results region for an htmx swap. The
  * templates in `views/fragments/` are the *same* templates in both cases; the page templates in `views/pages/` do
  * nothing but wrap them in the layout. Nothing is written twice, so nothing can drift.
  *
  * Two consequences fall out of that, and both are load-bearing:
  *
  *   - **Links stay shareable.** A permalink pasted into a new tab is a normal request and gets the whole page. The back
  *     button, `hx-boost` and a crawler all work with no special-casing.
  *   - **`Vary: HX-Request` is mandatory.** Two representations of one URL differing on a request header is exactly the
  *     situation `Vary` exists for; without it a shared proxy will serve a naked `<tbody>` to someone who typed the URL.
  *     [[varyOnHx]] is applied to every response from a dual-representation route, including error responses.
  *
  * The negative half of [[isFragment]] matters as much as the positive half. htmx re-fetches the full page when its
  * history cache misses, and it marks that request `HX-History-Restore-Request: true` while *also* sending
  * `HX-Request: true`. Answering it with a fragment replaces the entire document with a `<tbody>` and poisons every
  * subsequent back-navigation — a bug that only appears after a user navigates away and returns, which is to say never
  * during development.
  */
object Hx:

  /** Set on every request htmx issues. */
  val RequestHeader: String = "HX-Request"

  /** Set when htmx is restoring a history entry whose snapshot it no longer has. Full page, always. */
  val HistoryRestoreHeader: String = "HX-History-Restore-Request"

  /** The one client-event channel (ADR §8.4). Anything the page must react to travels as a named event here, never as a
    * sentinel element in the swapped markup.
    */
  val TriggerHeader: String = "HX-Trigger"

  /** Response header asking htmx to replace the browser URL, used when the server canonicalises a permalink. */
  val PushUrlHeader: String = "HX-Push-Url"

  val Vary: String = "Vary"

  private val True: String = "true"

  /** True when this request wants a bare fragment. See the class comment for why the history-restore case is excluded. */
  def isFragment(request: RequestHeader): Boolean =
    request.headers.get(RequestHeader).exists(_.equalsIgnoreCase(True)) &&
      !request.headers.get(HistoryRestoreHeader).exists(_.equalsIgnoreCase(True))

  extension (result: Result)

    /** Marks the response as varying on the fragment decision. Apply to every dual-representation route. */
    def varyOnHx: Result =
      val existing = result.header.headers.get(Vary).toVector.flatMap(_.split(',')).map(_.trim).filter(_.nonEmpty)
      val merged = (existing :+ RequestHeader).distinct.mkString(", ")
      result.withHeaders(Vary -> merged)

    /** Emits a client event. Kept behind this extension so the header name is spelled once. */
    def triggering(event: String): Result = result.withHeaders(TriggerHeader -> event)
