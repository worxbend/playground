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

import java.util.Base64
import scala.util.Try

/** An immutable byte string with *structural* equality.
  *
  * ADR §4.1 spells the binary payload `IArray[Byte]`. That type is an opaque alias over `Array`, so it inherits
  * `Array`'s reference equality: two `IArray`s with identical contents are `!=`. Threading that through `Payload` and
  * `Envelope` would make every derived `equals` wrong — and the things this build compares envelopes for are exactly
  * the load-bearing ones: codec round-trip properties, `(source, id)` deduplication and DLQ replay identity. A defect
  * there is silent, so the representation is wrapped instead.
  *
  * `toIArray` gives back the ADR's view for consumers that want it. The array is copied on the way in and on the way
  * out, which is the price of immutability that actually holds.
  */
final class Binary private (private val repr: Array[Byte]):

  /** Length in bytes — available without forcing a defensive copy. */
  def length: Int = repr.length

  def isEmpty: Boolean = repr.isEmpty

  /** A fresh mutable array. Callers may do as they like with it; the `Binary` is unaffected. */
  def toArray: Array[Byte] = repr.clone()

  /** The ADR's `IArray[Byte]` view. */
  def toIArray: IArray[Byte] = IArray.unsafeFromArray(repr.clone())

  /** RFC 4648 §4 base64 with padding — the encoding CloudEvents JSON Format 1.0 mandates for `data_base64`. */
  def base64: String = Base64.getEncoder.encodeToString(repr)

  override def equals(that: Any): Boolean = that match
    case other: Binary => java.util.Arrays.equals(repr, other.repr)
    case _             => false

  override def hashCode: Int = java.util.Arrays.hashCode(repr)

  /** Deliberately does not render the bytes: payloads reach logs through this method and may be sensitive. */
  override def toString: String = s"Binary(${repr.length} bytes)"

object Binary:

  val empty: Binary = new Binary(Array.emptyByteArray)

  /** Copies, so a later mutation of `bytes` cannot retroactively change a stored event. */
  def copyOf(bytes: Array[Byte]): Binary = new Binary(bytes.clone())

  def from(bytes: IArray[Byte]): Binary = new Binary(IArray.genericWrapArray(bytes).toArray)

  /** Strict base64: `Base64.getDecoder` rejects trailing garbage, which is what we want for a wire format. The throw is
    * converted here so decoding an event is never an exception at the call site.
    */
  def fromBase64(encoded: String): Either[String, Binary] =
    Try(Base64.getDecoder.decode(encoded)).toEither.left
      .map(e => s"data_base64 is not valid base64: ${e.getMessage}")
      .map(new Binary(_))
