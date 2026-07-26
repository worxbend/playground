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

import java.net.URI
import java.time.OffsetDateTime

import io.circe.Json

/** The CloudEvents attribute type system, as an ADT rather than `Map[String, String]`.
  *
  * The spec defines exactly six attribute types (Boolean, Integer, String, Binary, URI, URI-Reference, Timestamp — the
  * two URI forms share a Scala representation here). Collapsing them to strings would be lossy in the direction that
  * matters: a producer that sets `sequence` as an Integer and a consumer that reads it as a string disagree about
  * ordering, and nothing in the pipeline would notice.
  *
  * `Other` is not in the ADR. It exists because losslessness is a hard requirement and the JSON format cannot be made
  * total without it: a non-integral number, an array or an object appearing as an extension value is out of spec, but
  * dropping it — or failing the whole event because of it — loses data this system promised to keep. `Other` carries
  * such a value verbatim so it round-trips and is still visible in search.
  */
enum AttrValue:
  case Text(v: String)
  case Num(v: Int)
  case Flag(v: Boolean)
  case Time(v: OffsetDateTime)
  case Ref(v: URI)
  case Bytes(v: Binary)
  case Other(v: Json)

  /** The JSON Format 1.0 rendering.
    *
    * Only Boolean and Integer have non-string JSON forms; everything else is a string, which is why the format is
    * *type-erasing* in one direction — see [[canonical]].
    */
  def toJson: Json = this match
    case Text(v)  => Json.fromString(v)
    case Num(v)   => Json.fromInt(v)
    case Flag(v)  => Json.fromBoolean(v)
    case Time(v)  => Json.fromString(v.toString)
    case Ref(v)   => Json.fromString(v.toString)
    case Bytes(v) => Json.fromString(v.base64)
    case Other(v) => v

  /** The value as it will come back out of [[AttrValue.fromJson]] after a JSON round-trip.
    *
    * `Time`, `Ref` and `Bytes` all serialise to plain JSON strings and are indistinguishable from `Text` on the way
    * back, because JSON Format 1.0 carries no per-extension type information — the receiver is expected to know. This
    * makes that erasure explicit and testable instead of leaving it as a surprise in a round-trip property.
    */
  def canonical: AttrValue = this match
    case Time(v)  => Text(v.toString)
    case Ref(v)   => Text(v.toString)
    case Bytes(v) => Text(v.base64)
    case Other(v) => AttrValue.fromJson(v)
    case scalar   => scalar

object AttrValue:

  /** Total: every JSON value maps to some `AttrValue`, so an unknown extension never fails an event.
    *
    * A number becomes `Num` only when its literal text is exactly the `Int`'s own rendering. `5.0` and `5e0` are
    * numerically 5 but would re-serialise as `5`, which is a lexical change to a value we promised to preserve — those
    * go to `Other` and come back byte-identical.
    */
  def fromJson(json: Json): AttrValue =
    json.asString
      .map(Text.apply)
      .orElse(json.asBoolean.map(Flag.apply))
      .orElse(json.asNumber.flatMap(n => n.toInt.filter(i => i.toString == n.toString)).map(Num.apply))
      .getOrElse(Other(json))
