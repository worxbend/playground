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

package com.worxbend.ferrite.web

import com.worxbend.kernel.search.PercentCodec

/** Query-string editing for the filter bar, the facet panel and the "load more" cursor.
  *
  * The UI's whole navigation model is "take the URL the user is on and change one parameter": clicking a facet adds
  * `&device=kitchen-1`, dismissing a chip removes it, paging appends a cursor. Doing that on the *string* rather than
  * on the [[com.worxbend.kernel.search.Filter]] AST is deliberate — the AST cannot represent a half-built or invalid
  * filter, and the one thing this UI must never do is silently drop a parameter it could not parse (ADR §6.3). Editing
  * pairs keeps a malformed value visible in the URL and in the filter bar, where the user can fix it.
  *
  * **The escaping is [[com.worxbend.kernel.search.PercentCodec]]'s and is not restated here.** There is one permalink
  * escaping rule and the module that owns the format owns it. This object used to carry its own copy, written from that
  * codec's Scaladoc and claiming to match it; the two then disagreed about astral characters — an emoji in a `q=` term
  * decoded to `??` here and to itself there — for as long as nobody compared them. What is local to this layer is only
  * the *reading* of a broken escape: the filter bar always needs a string to put back in front of the user, so
  * [[decode]] is `decodeLenient` and the strict reading stays with the codec, where a mangled fragment becomes a
  * reportable error.
  */
object Query:

  /** Splits a raw query string into decoded pairs, preserving order and repeats.
    *
    * Total: a fragment whose percent-encoding is broken keeps its raw text rather than being dropped, because dropping
    * it would quietly narrow the search the URL describes. The kernel's decoder sees the same fragment and reports it.
    */
  def parse(raw: String): Vector[(String, String)] =
    raw
      .stripPrefix("?")
      .split('&')
      .iterator
      .filter(_.nonEmpty)
      .map { fragment =>
        val separator = fragment.indexOf('=')
        val (key, value) =
          if separator < 0 then (fragment, "") else (fragment.take(separator), fragment.drop(separator + 1))
        (decode(key), decode(value))
      }
      .toVector

  /** Renders pairs back into a query string, without the leading `?`. */
  def render(pairs: Vector[(String, String)]): String =
    pairs.map((key, value) => s"${encode(key)}=${encode(value)}").mkString("&")

  /** Drops every occurrence of `key`. */
  def remove(pairs: Vector[(String, String)], key: String): Vector[(String, String)] =
    pairs.filterNot((k, _) => k == key)

  /** Drops one specific `key=value` occurrence, leaving other values of the same key alone. */
  def remove(pairs: Vector[(String, String)], key: String, value: String): Vector[(String, String)] =
    pairs.filterNot((k, v) => k == key && v == value)

  /** Replaces every occurrence of `key` with a single pair. Used for the single-valued controls (`limit`, `sort`). */
  def set(pairs: Vector[(String, String)], key: String, value: String): Vector[(String, String)] =
    remove(pairs, key) :+ (key -> value)

  /** Adds `key=value` unless it is already present — clicking an already-selected facet must not double it. */
  def add(pairs: Vector[(String, String)], key: String, value: String): Vector[(String, String)] =
    if pairs.contains(key -> value) then pairs else pairs :+ (key -> value)

  /** Facet click semantics: selected values deselect, unselected values select. */
  def toggle(pairs: Vector[(String, String)], key: String, value: String): Vector[(String, String)] =
    if pairs.contains(key -> value) then remove(pairs, key, value) else add(pairs, key, value)

  def encode(raw: String): String = PercentCodec.encode(raw)

  /** Lenient by design: an invalid escape is kept verbatim rather than raising, so the caller always gets a string to
    * put back in front of the user. The kernel's strict reading of the same grammar reports it instead.
    */
  def decode(raw: String): String = PercentCodec.decodeLenient(raw)
