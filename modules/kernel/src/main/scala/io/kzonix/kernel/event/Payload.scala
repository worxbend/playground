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

package io.kzonix.kernel.event

import io.circe.Json

/** The `data` slot of a CloudEvent.
  *
  * JSON Format 1.0 offers exactly three shapes and they are mutually exclusive on the wire: an inline `data` JSON
  * value, a base64 `data_base64` string, or neither. Modelling that as an ADT rather than as two nullable fields makes
  * "both present" — which the spec forbids — unrepresentable rather than a validation rule someone forgets.
  *
  * `Structured` holds an arbitrary `Json`, not a decoded domain type. That is the load-bearing choice of ADR §4.2: an
  * event whose shape this build has never seen still parses, still persists and still renders.
  */
enum Payload:
  case Structured(json: Json)
  case Opaque(bytes: Binary, mediaType: ContentType)
  case Empty

  /** True when the payload can be projected into `jsonb` without re-encoding. Binary payloads are stored as their
    * base64 form because Postgres `jsonb` has no byte type.
    */
  def isJson: Boolean = this match
    case Structured(_) => true
    case _             => false

object Payload:

  /** Convenience for the overwhelmingly common case. */
  def json(value: Json): Payload = Structured(value)
