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

import java.nio.charset.StandardCharsets.UTF_8
import scala.annotation.tailrec

/** Query-string editing for the filter bar, the facet panel and the "load more" cursor.
  *
  * The UI's whole navigation model is "take the URL the user is on and change one parameter": clicking a facet adds
  * `&device=kitchen-1`, dismissing a chip removes it, paging appends a cursor. Doing that on the *string* rather than
  * on the [[com.worxbend.kernel.search.Filter]] AST is deliberate — the AST cannot represent a half-built or invalid
  * filter, and the one thing this UI must never do is silently drop a parameter it could not parse (ADR §6.3). Editing
  * pairs keeps a malformed value visible in the URL and in the filter bar, where the user can fix it.
  *
  * Encoding matches `FilterQuery`'s own codec in `modules/kernel`: the same safe set, so `:` and `/` in a CloudEvents
  * `source` stay legible, and `+` is always escaped on the way out and always read as a space on the way in. It is
  * re-implemented rather than shared because that codec is private to the kernel and exporting it would put a URL
  * concern into the domain module.
  */
object Query:

  private val Safe: Set[Char] = (('A' to 'Z') ++ ('a' to 'z') ++ ('0' to '9')).toSet ++
    Set('-', '.', '_', '~', ':', '/', '@', '*')

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

  def encode(raw: String): String =
    raw
      .getBytes(UTF_8)
      .iterator
      .map { byte =>
        val unsigned = byte & 0xff
        if Safe(unsigned.toChar) then unsigned.toChar.toString else f"%%$unsigned%02X"
      }
      .mkString

  /** Lenient by design: an invalid escape is kept verbatim rather than raising, so the caller always gets a string to
    * put back in front of the user.
    */
  def decode(raw: String): String =
    @tailrec def go(index: Int, acc: Vector[Byte]): Vector[Byte] =
      if index >= raw.length then acc
      else
        raw.charAt(index) match
          case '%' if index + 2 < raw.length && hexByte(raw.charAt(index + 1), raw.charAt(index + 2)).isDefined =>
            go(index + 3, acc ++ hexByte(raw.charAt(index + 1), raw.charAt(index + 2)).toVector)
          case '+' => go(index + 1, acc :+ ' '.toByte)
          case ch  => go(index + 1, acc ++ ch.toString.getBytes(UTF_8).toVector)

    String(go(0, Vector.empty).toArray, UTF_8)

  private def hexByte(high: Char, low: Char): Option[Byte] =
    val h = Character.digit(high, 16)
    val l = Character.digit(low, 16)
    if h < 0 || l < 0 then None else Some(((h << 4) | l).toByte)
