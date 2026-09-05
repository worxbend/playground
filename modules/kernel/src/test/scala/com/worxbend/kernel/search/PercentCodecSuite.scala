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

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

/** The permalink's escaping rule.
  *
  * This codec exists as a public type because it was duplicated once and the two copies disagreed. The properties
  * below are the contract the second copy failed: a round trip is the identity for *any* string, and the two readings
  * of a broken escape agree on everything the strict one accepts.
  */
final class PercentCodecSuite extends munit.ScalaCheckSuite:

  private val genText: Gen[String] =
    Gen.oneOf(
      Gen.asciiPrintableStr,
      Gen.alphaNumStr,
      Gen.const("com.worxbend.iot.telemetry"),
      Gen.const("https://home.example/a b?c=d&e"),
      Gen.const("smoke 🔥 kitchen 🧯"),
      Gen.const("100%"),
      Gen.const("a+b"),
      Gen.const("zażółć gęślą jaźń")
    )

  test("the characters a CloudEvents source is made of stay legible"):
    assertEquals(PercentCodec.encode("https://home.example/gateways/1"), "https://home.example/gateways/1")

  test("a space is escaped rather than turned into a plus, and a plus survives as itself"):
    assertEquals(PercentCodec.encode("a b"), "a%20b")
    assertEquals(PercentCodec.encode("a+b"), "a%2Bb")
    assertEquals(PercentCodec.decode("a+b"), Right("a b"))

  test("characters outside the basic multilingual plane survive a decode"):
    // The bug this codec was made public to stop having twice: encoding one UTF-16 unit at a time turns each half of
    // a surrogate pair into `?`, so `q=🔥` silently becomes `q=??` and searches for something nobody typed.
    assertEquals(PercentCodec.decodeLenient("smoke 🔥"), "smoke 🔥")
    assertEquals(PercentCodec.decode("smoke 🔥"), Right("smoke 🔥"))
    assertEquals(PercentCodec.decodeLenient("%F0%9F%94%A5"), "🔥")

  test("strict decoding names the broken escape; lenient decoding keeps it visible"):
    assert(PercentCodec.decode("a%2").isLeft)
    assert(PercentCodec.decode("a%zz").isLeft)
    assertEquals(PercentCodec.decodeLenient("a%2"), "a%2")
    assertEquals(PercentCodec.decodeLenient("100% sure"), "100% sure")

  property("encode then decode is the identity"):
    forAll(genText): text =>
      PercentCodec.decode(PercentCodec.encode(text)) == Right(text)

  property("the two readings agree wherever the strict one succeeds"):
    forAll(Gen.asciiPrintableStr): raw =>
      PercentCodec.decode(raw).forall(_ == PercentCodec.decodeLenient(raw))

  property("an encoded string contains nothing that could re-split a query string"):
    forAll(genText): text =>
      val encoded = PercentCodec.encode(text)
      !encoded.exists(ch => ch == '&' || ch == '=' || ch == '?' || ch == '#' || ch == '+')
