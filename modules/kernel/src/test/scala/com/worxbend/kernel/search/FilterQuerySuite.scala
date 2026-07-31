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

import io.circe.Json
import java.time.OffsetDateTime
import org.scalacheck.Prop.forAll

/** The permalink codec.
  *
  * A permalink is a durable artefact: it goes into bookmarks, incident tickets and chat messages, and it is expected to
  * reproduce the same result set months later. The round-trip property is therefore the whole test — everything else
  * here pins down the readable spelling ADR §6.3 promises, and the failure modes that must stay visible instead of
  * turning into an unfiltered result set.
  */
final class FilterQuerySuite extends munit.ScalaCheckSuite:

  override def scalaCheckTestParameters = super.scalaCheckTestParameters.withMinSuccessfulTests(300)

  property("encoding then decoding a flat filter is the identity"):
    forAll(FilterGenerators.genFlatFilter): filter =>
      FilterQuery.encode(Some(filter)).map(FilterQuery.decode) == Right(Right(Some(filter)))

  property("encoding is deterministic, so a content hash over it is stable"):
    forAll(FilterGenerators.genFlatFilter): filter =>
      FilterQuery.encode(Some(filter)) == FilterQuery.encode(Some(filter))

  property("branch order in the AST does not change the permalink"):
    forAll(FilterGenerators.genFlatFilter): filter =>
      val reordered = Filter.and(Filter.leaves(filter).reverse)
      reordered.toOption.map(f => FilterQuery.encode(Some(f))) == Some(FilterQuery.encode(Some(filter)))

  property("decoding never throws, whatever the input"):
    forAll(org.scalacheck.Gen.asciiPrintableStr): raw =>
      scala.util.Try(FilterQuery.decode(raw)).isSuccess

  property("every encoded permalink carries its version"):
    forAll(FilterGenerators.genFlatFilter): filter =>
      FilterQuery.encode(Some(filter)).exists(_.startsWith("v=1"))

  test("the empty filter is a valid permalink, not an error"):
    assertEquals(FilterQuery.encode(None), Right("v=1"))
    assertEquals(FilterQuery.decode("v=1"), Right(None))
    assertEquals(FilterQuery.decode("?v=1"), Right(None))

  test("the spelling is the readable one the ADR shows"):
    val filter = FilterGenerators.force(
      Filter.and(
        Vector(
          FilterGenerators.force(Filter.occurred(Some(OffsetDateTime.parse("2026-07-01T00:00:00Z")), None)),
          FilterGenerators.force(Filter.typeIn(Vector("com.worxbend.iot.alarm"))),
          FilterGenerators.force(Filter.deviceIn(Vector("smoke-1"))),
          FilterGenerators.force(Filter.tagsAll(Vector("indoor"))),
          Filter.severityAtLeast(Severity.Warn),
          FilterGenerators.force(Filter.payloadCmp("temperature", NumOp.Gt, BigDecimal(21))),
          FilterGenerators.force(Filter.extensionEq("tenantid", "acme")),
          FilterGenerators.force(Filter.fullText("smoke"))
        )
      )
    )
    assertEquals(
      FilterQuery.encode(Some(filter)),
      Right(
        "v=1&from=2026-07-01T00:00:00Z&type=com.worxbend.iot.alarm&device=smoke-1&severity=%3E%3Dwarn" +
          "&tag=indoor&data.temperature=%3E21&ext.tenantid=acme&q=smoke"
      )
    )

  test("a hand-written permalink parses"):
    val decoded = FilterQuery.decode("v=1&type=com.worxbend.iot.alarm&severity=>=warn&device=a&device=b&q=smoke")
    val expected = Filter.and(
      Vector(
        FilterGenerators.force(Filter.typeIn(Vector("com.worxbend.iot.alarm"))),
        FilterGenerators.force(Filter.deviceIn(Vector("a", "b"))),
        Filter.severityAtLeast(Severity.Warn),
        FilterGenerators.force(Filter.fullText("smoke"))
      )
    )
    assertEquals(decoded, Right(Some(FilterGenerators.force(expected))))

  test("a bare severity is accepted as well as the >= spelling"):
    assertEquals(FilterQuery.decode("v=1&severity=warn"), FilterQuery.decode("v=1&severity=>=warn"))

  test("repeated dimension parameters union into one leaf"):
    assertEquals(
      FilterQuery.decode("v=1&type=a&type=b&type=a"),
      Right(Some(FilterGenerators.force(Filter.typeIn(Vector("a", "b")))))
    )

  test("a missing or unknown version is reported rather than guessed"):
    assertEquals(FilterQuery.decode("type=a"), Left(Vector(FilterError.MissingVersion)))
    assertEquals(FilterQuery.decode("v=2&type=a"), Left(Vector(FilterError.UnsupportedVersion("2"))))

  test("an unknown parameter is surfaced, never silently dropped"):
    assertEquals(FilterQuery.decode("v=1&colour=red"), Left(Vector(FilterError.UnknownParameter("colour"))))

  test("a repeated single-valued parameter is an error rather than an arbitrary winner"):
    assertEquals(FilterQuery.decode("v=1&q=a&q=b"), Left(Vector(FilterError.Repeated("q"))))

  test("an invalid value is reported against its own parameter"):
    assertEquals(
      FilterQuery.decode("v=1&from=yesterday"),
      Left(Vector(FilterError.Invalid("from", "'yesterday' is not an RFC 3339 timestamp")))
    )
    assert(FilterQuery.decode("v=1&severity=loud").isLeft)
    assert(FilterQuery.decode("v=1&tag=not%20a%20tag").isLeft)
    assert(FilterQuery.decode("v=1&data.temperature=warm").isLeft)
    assert(FilterQuery.decode("v=1&ext.Tenant=acme").isLeft)
    assert(FilterQuery.decode("v=1&data=notjson").isLeft)

  test("an inverted time range is rejected at the codec boundary, not turned into an empty result"):
    assert(FilterQuery.decode("v=1&from=2026-08-01T00:00:00Z&until=2026-07-01T00:00:00Z").isLeft)

  test("all errors in one link are reported together"):
    val errors = FilterQuery.decode("v=1&colour=red&from=yesterday").swap.getOrElse(Vector.empty)
    assertEquals(errors.size, 2)
    assert(errors.contains(FilterError.UnknownParameter("colour")))

  test("broken percent encoding is reported against the fragment"):
    assert(FilterQuery.decode("v=1&q=%ZZ").isLeft)
    assert(FilterQuery.decode("v=1&q=%A").isLeft)

  test("a filter with Or or Not has no permalink and says so"):
    val a = FilterGenerators.force(Filter.typeIn(Vector("a")))
    val b = FilterGenerators.force(Filter.deviceIn(Vector("b")))
    assert(FilterQuery.encode(Filter.or(Vector(a, b)).toOption).isLeft)
    assert(FilterQuery.encode(Some(Filter.not(a))).isLeft)

  test("payload containment survives as compact JSON"):
    val filter = FilterGenerators.force(
      Filter.payloadContains(Json.obj("room" -> Json.fromString("kitchen"), "state" -> Json.fromString("open")))
    )
    val encoded = FilterQuery.encode(Some(filter))
    assert(encoded.exists(_.contains("data=")))
    assertEquals(encoded.map(FilterQuery.decode), Right(Right(Some(filter))))

  test("values containing reserved URL characters round-trip"):
    val filter = FilterGenerators.force(Filter.sourceIn(Vector("https://home.example/a b?c=d&e", "x+y", "100%")))
    assertEquals(FilterQuery.encode(Some(filter)).map(FilterQuery.decode), Right(Right(Some(filter))))

  test("a URI keeps its colons and slashes so the link stays hand-editable"):
    val filter = FilterGenerators.force(Filter.sourceIn(Vector("https://home.example/gateway/1")))
    assertEquals(FilterQuery.encode(Some(filter)), Right("v=1&source=https://home.example/gateway/1"))

  test("a plus sign in a hand-typed link decodes as a space, as form encoding implies"):
    assertEquals(
      FilterQuery.decode("v=1&q=smoke+detected"),
      Right(Some(FilterGenerators.force(Filter.fullText("smoke detected"))))
    )
