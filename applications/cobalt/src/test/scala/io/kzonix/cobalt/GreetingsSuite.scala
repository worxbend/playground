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

package io.kzonix.cobalt

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

/** MUnit + ScalaCheck, both supplied as common test dependencies. */
final class GreetingsSuite extends munit.ScalaCheckSuite:

  test("health reports UP"):
    assertEquals(Greetings.health("status").str, "UP")

  test("greet renders the supplied name"):
    assertEquals(Greetings.greet("world")("message").str, "Hello, world")

  test("blank names are rejected"):
    assert(Greetings.validateName("   ").isLeft)
    assert(Greetings.validateName("").isLeft)

  property("any non-blank name is accepted and echoed back trimmed"):
    forAll(Gen.alphaNumStr.suchThat(_.trim.nonEmpty)): name =>
      val validated = Greetings.validateName(s"  $name  ")
      validated == Right(name.trim) &&
      Greetings.greet(name)("message").str == s"Hello, ${name.trim}"
