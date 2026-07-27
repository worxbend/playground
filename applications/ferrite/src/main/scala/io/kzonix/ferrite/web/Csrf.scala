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
import play.filters.csrf.CSRF

/** The CSRF token, as a plain string a template can put in an attribute.
  *
  * ADR §8.4 puts the token once on `<body hx-headers='{"Csrf-Token":"…"}'>`: htmx inherits `hx-headers` down the DOM,
  * so every request any descendant issues carries it, and — unlike a hidden input — it survives an `innerHTML` swap of
  * the region the form lives in. `Csrf-Token` is Play's default header name, so no configuration is needed.
  *
  * This wrapper exists instead of calling `views.html.helper.CSRF.getToken(request).value` directly in the layout for
  * one reason: that helper **throws** when no token is present. A token is absent whenever the CSRF filter did not run
  * — a unit test rendering a template against a bare `FakeRequest`, or a misconfigured `play.filters.disabled`. A
  * template that throws in that situation converts a configuration mistake into a 500 on every page, which is a much
  * worse signal than an empty attribute plus a rejected POST.
  */
object Csrf:

  /** Play's default header name for the token. Spelled once, here. */
  val HeaderName: String = "Csrf-Token"

  /** The token value, or the empty string when the filter did not install one. */
  def token(request: RequestHeader): String = CSRF.getToken(request).fold("")(_.value)

  /** The `hx-headers` attribute value: a JSON object with the token under [[HeaderName]].
    *
    * Built here rather than interpolated in the template so the JSON is well-formed by construction. The token itself
    * is base64url and contains nothing needing JSON escaping, and Twirl escapes the attribute for HTML on the way out.
    */
  def hxHeaders(request: RequestHeader): String = s"""{"$HeaderName": "${token(request)}"}"""
