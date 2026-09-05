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

package com.worxbend.kernel.search

import java.nio.charset.StandardCharsets.UTF_8
import scala.annotation.tailrec

/** Percent-encoding for the permalink (ADR §6.3).
  *
  * Hand-rolled rather than `java.net.URLEncoder` for one reason: `URLEncoder` escapes `:` and `/`, which turns every
  * `source` and `dataschema` in a link into unreadable noise and defeats the "hand-editable" requirement. [[Safe]]
  * keeps those legible while escaping everything that could change how the string parses. `+` is always escaped on the
  * way out and always decoded as a space on the way in, which is what a human pasting a form-encoded URL expects and
  * still round-trips exactly.
  *
  * **Public, and in the kernel, because there is exactly one permalink escaping rule and two modules need it.** This
  * used to be private to `FilterQuery`, and the web tier re-implemented it from the Scaladoc — which is how the two
  * copies came to disagree about astral characters while each claimed to match the other. A safe set and an escape
  * grammar are a single design decision; whoever owns the format owns the codec. It is pure `java.lang`, so it costs
  * the kernel none of its framework-freedom.
  *
  * Two readings of a broken escape, because the two callers genuinely need different ones and inventing a second codec
  * to get the other is what caused the drift:
  *
  *   - [[decode]] is strict. `FilterQuery` reports a mangled fragment as a [[FilterError.Malformed]] rather than
  *     guessing, because guessing would run a search the URL did not ask for.
  *   - [[decodeLenient]] keeps a broken escape verbatim. The filter bar must always have a string to put back in front
  *     of the user, including the invalid one they need to see in order to fix it.
  *
  * They agree on every input the strict one accepts.
  */
object PercentCodec:

  /** Unescaped on the way out. Beyond the unreserved set of RFC 3986 it keeps `:`, `/`, `@` and `*`, which appear in
    * every CloudEvents `source` and cannot change how a query string parses.
    */
  val Safe: Set[Char] = (('A' to 'Z') ++ ('a' to 'z') ++ ('0' to '9')).toSet ++
    Set('-', '.', '_', '~', ':', '/', '@', '*')

  def encode(raw: String): String =
    val pieces = raw.getBytes(UTF_8).iterator.map { byte =>
      val unsigned = byte & 0xff
      if Safe(unsigned.toChar) then unsigned.toChar.toString else f"%%$unsigned%02X"
    }
    pieces.mkString

  /** Strict: a truncated or non-hex escape is a failure, with the offending text in the message. */
  def decode(raw: String): Either[String, String] = validate(raw).map(_ => decodeLenient(raw))

  /** Lenient: a broken escape is kept as the literal `%` that introduced it, so this is total. */
  def decodeLenient(raw: String): String =
    @tailrec def go(index: Int, acc: Vector[Byte]): Vector[Byte] =
      if index >= raw.length then acc
      else
        raw.charAt(index) match
          case '%' =>
            escapeAt(raw, index) match
              case Some(byte) => go(index + 3, acc :+ byte)
              case None       => go(index + 1, acc :+ '%'.toByte)
          case '+' => go(index + 1, acc :+ ' '.toByte)
          case ch  =>
            // A code point, not a `char`. Encoding a lone surrogate as UTF-8 yields `?`, so taking one UTF-16 unit at
            // a time turned every astral character — an emoji in a `q=` term — into two question marks, silently
            // changing the search a hand-written link asked for. Browsers percent-encode the query, which is why this
            // only ever bit a URL pasted from a chat client or built by hand.
            val paired =
              Character.isHighSurrogate(ch) && index + 1 < raw.length && Character.isLowSurrogate(raw.charAt(index + 1))
            val next = if paired then index + 2 else index + 1
            go(next, acc ++ raw.substring(index, next).getBytes(UTF_8).toVector)

    String(go(0, Vector.empty).toArray, UTF_8)

  @tailrec private def validate(raw: String, index: Int = 0): Either[String, Unit] =
    if index >= raw.length then Right(())
    else if raw.charAt(index) != '%' then validate(raw, index + 1)
    else if index + 2 >= raw.length then Left(s"truncated percent escape at $index")
    else if escapeAt(raw, index).isEmpty then Left(s"invalid percent escape '${raw.substring(index, index + 3)}'")
    else validate(raw, index + 3)

  /** The byte a `%XX` at `index` denotes, or `None` if it is truncated or not hexadecimal. */
  private def escapeAt(raw: String, index: Int): Option[Byte] =
    if index + 2 >= raw.length then None
    else
      val high = Character.digit(raw.charAt(index + 1), 16)
      val low = Character.digit(raw.charAt(index + 2), 16)
      if high < 0 || low < 0 then None else Some(((high << 4) | low).toByte)
