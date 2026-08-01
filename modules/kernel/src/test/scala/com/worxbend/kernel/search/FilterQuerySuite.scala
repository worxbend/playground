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

  /** The message `encode` gives when two leaves want the same parameter. Spelled once so the tests below read as the
    * cases they are rather than as repeated string literals.
    */
  private def clash(key: String): String =
    s"a query string holds one filter per parameter, and $key is asked for twice"

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

  test("two leaves of one family have no permalink, rather than a link that widens the filter"):
    // `And(TypeIn(a), TypeIn(b))` is an intersection. Flattened into `type=a&type=b` it decodes as the *union*, so the
    // link showed rows the filter it was built from excludes — the one outcome this codec's totality rules exist to
    // prevent. Reported the same way `Or` and `Not` are, which routes it to a saved search (ADR §6.3).
    val types = Filter.and(
      Vector(
        FilterGenerators.force(Filter.typeIn(Vector("a"))),
        FilterGenerators.force(Filter.typeIn(Vector("b")))
      )
    )
    assertEquals(FilterQuery.encode(types.toOption), Left(FilterError.NotPermalinkable(clash("type"))))
    // Two of the same *single-valued* family used to encode into a link `decode` then refused as `Repeated` — an
    // unopenable permalink rather than a wrong one, and just as broken.
    val severities =
      Filter.and(Vector(Filter.severityAtLeast(Severity.Warn), Filter.severityAtLeast(Severity.Error)))
    assertEquals(FilterQuery.encode(severities.toOption), Left(FilterError.NotPermalinkable(clash("severity"))))
    // A time window split across two leaves is *semantically* the window `decode` rebuilds, but it is not the same
    // AST, so the round trip is not an identity and the codec says so instead of quietly canonicalising.
    val split = Filter.and(
      Vector(
        FilterGenerators.force(Filter.occurred(Some(OffsetDateTime.parse("2026-07-01T00:00:00Z")), None)),
        FilterGenerators.force(Filter.occurred(None, Some(OffsetDateTime.parse("2026-08-01T00:00:00Z"))))
      )
    )
    assert(FilterQuery.encode(split.toOption).isLeft)
    // Two equalities on one extension name collide; two names are an ordinary conjunction and still encode.
    val sameName = Filter.and(
      Vector(
        FilterGenerators.force(Filter.extensionEq("tenantid", "acme")),
        FilterGenerators.force(Filter.extensionEq("tenantid", "other"))
      )
    )
    assertEquals(FilterQuery.encode(sameName.toOption), Left(FilterError.NotPermalinkable(clash("ext.tenantid"))))
    val twoNames = Filter.and(
      Vector(
        FilterGenerators.force(Filter.extensionEq("tenantid", "acme")),
        FilterGenerators.force(Filter.extensionEq("sequence", "7"))
      )
    )
    assert(FilterQuery.encode(twoNames.toOption).isRight)

  test("a repeated payload comparison is not a clash, because a range is how the grammar spells it"):
    val range = Filter.and(
      Vector(
        FilterGenerators.force(Filter.payloadCmp("temperature", NumOp.Gte, BigDecimal(18))),
        FilterGenerators.force(Filter.payloadCmp("temperature", NumOp.Lt, BigDecimal(24)))
      )
    )
    assertEquals(FilterQuery.encode(range.toOption).map(FilterQuery.decode), Right(Right(range.toOption)))

  test("a hand-written link keeps characters outside the basic plane"):
    // `q=🔥` arrives raw from anything that is not a browser address bar. Decoding UTF-16 unit by unit turned the
    // surrogate pair into two `?`, which searched for something the user did not type and reported no error.
    assertEquals(
      FilterQuery.decode("v=1&q=smoke 🔥"),
      Right(Some(FilterGenerators.force(Filter.fullText("smoke 🔥"))))
    )
    val filter = FilterGenerators.force(Filter.fullText("🔥 kitchen 🧯"))
    assertEquals(FilterQuery.encode(Some(filter)).map(FilterQuery.decode), Right(Right(Some(filter))))

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

  // ---------------------------------------------------------------------------------------------------------------
  // The two open-ended families: `data.<path>` and `ext.<name>`.
  //
  // These are the only keys the grammar does not fix, so they are the only ones where an unrecognised parameter and a
  // valid one are told apart by a prefix rather than by a lookup. Every test below is about that boundary.
  // ---------------------------------------------------------------------------------------------------------------

  test("a payload comparison is a prefixed key with the operator leading the value"):
    val filter = FilterGenerators.force(Filter.payloadCmp("sensor.temperature", NumOp.Gte, BigDecimal("21.5")))
    assertEquals(FilterQuery.encode(Some(filter)), Right("v=1&data.sensor.temperature=%3E%3D21.5"))
    assertEquals(FilterQuery.decode("v=1&data.sensor.temperature=>=21.5"), Right(Some(filter)))

  test("every comparison operator survives the round trip, including the two human spellings"):
    NumOp.values.foreach: op =>
      val filter = FilterGenerators.force(Filter.payloadCmp("value", op, BigDecimal(3)))
      assertEquals(FilterQuery.encode(Some(filter)).map(FilterQuery.decode), Right(Right(Some(filter))), op.toString)
    // `=` and `<>` are what a person types; they mean the jsonpath `==` and `!=` the encoder writes back.
    assertEquals(FilterQuery.decode("v=1&data.value=%3D3"), FilterQuery.decode("v=1&data.value=%3D%3D3"))
    assertEquals(FilterQuery.decode("v=1&data.value=%3C%3E3"), FilterQuery.decode("v=1&data.value=%21%3D3"))

  test("a bare number with no operator means equality, which is what a person typing a URL expects"):
    assertEquals(
      FilterQuery.decode("v=1&data.value=3"),
      Right(Some(FilterGenerators.force(Filter.payloadCmp("value", NumOp.Eq, BigDecimal(3)))))
    )

  test("the permalink carries the plain decimal form, never scientific notation"):
    // `BigDecimal#toString` would write `1E+8` here. `+` percent-encodes to `%2B`, so the readable half of the
    // grammar would be three characters of noise for a number the user typed as 100000000.
    val filter = FilterGenerators.force(Filter.payloadCmp("value", NumOp.Gt, BigDecimal("1E+8")))
    assertEquals(FilterQuery.encode(Some(filter)), Right("v=1&data.value=%3E100000000"))
    assertEquals(FilterQuery.decode("v=1&data.value=>100000000"), Right(Some(filter)))

  test("several comparisons on one path are a range, not a repeated parameter"):
    val decoded = FilterQuery.decode("v=1&data.temperature=>=18&data.temperature=<24")
    val expected = Filter.and(
      Vector(
        FilterGenerators.force(Filter.payloadCmp("temperature", NumOp.Gte, BigDecimal(18))),
        FilterGenerators.force(Filter.payloadCmp("temperature", NumOp.Lt, BigDecimal(24)))
      )
    )
    assertEquals(decoded, Right(Some(FilterGenerators.force(expected))))
    assertEquals(
      decoded.map(f => FilterQuery.encode(f)),
      Right(Right("v=1&data.temperature=%3E%3D18&data.temperature=%3C24"))
    )

  test("two values for one extension name are reported, not silently conjoined into an impossible filter"):
    // The grammar has only equality on an extension, so `ext.tenantid=a&ext.tenantid=b` can match nothing. Building it
    // would return an empty page that reads as "nothing matched" rather than "this link contradicts itself".
    assertEquals(
      FilterQuery.decode("v=1&ext.tenantid=acme&ext.tenantid=other"),
      Left(Vector(FilterError.Repeated("ext.tenantid")))
    )
    // Two *different* names are an ordinary conjunction.
    assert(FilterQuery.decode("v=1&ext.tenantid=acme&ext.sequence=7").isRight)

  test("an extension value keeps every character a CloudEvents attribute may contain"):
    val traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    val filter = FilterGenerators.force(Filter.extensionEq("traceparent", traceparent))
    assertEquals(FilterQuery.encode(Some(filter)).map(FilterQuery.decode), Right(Right(Some(filter))))
    val awkward = FilterGenerators.force(Filter.extensionEq("tenantid", "a=b&c d%e+f"))
    assertEquals(FilterQuery.encode(Some(awkward)).map(FilterQuery.decode), Right(Right(Some(awkward))))

  test("a malformed prefixed key is reported against itself, never dropped"):
    // Dropping any of these would widen the result set past what the URL asked for, which is the failure mode this
    // codec exists to prevent. Each is reported against the key the user can see and edit.
    def reason(query: String): Vector[FilterError] = FilterQuery.decode(query).swap.getOrElse(Vector.empty)
    assert(reason("v=1&data.temperature=warm").exists {
      case FilterError.Invalid("data.temperature", _) => true; case _ => false
    })
    assert(reason("v=1&data.=>1").exists { case FilterError.Invalid("data.", _) => true; case _ => false })
    assert(reason("v=1&data.a%20b=>1").exists { case FilterError.Invalid("data.a b", _) => true; case _ => false })
    assert(reason("v=1&data.value=%3E%3C1").exists {
      case FilterError.Invalid("data.value", _) => true; case _ => false
    })
    assert(reason("v=1&ext.Tenant=acme").exists { case FilterError.Invalid("ext.Tenant", _) => true; case _ => false })
    assert(reason("v=1&ext.tenantid=").exists { case FilterError.Invalid("ext.tenantid", _) => true; case _ => false })
    // A near miss on the prefix itself is an unknown parameter, not a payload filter with an odd name.
    assertEquals(FilterQuery.decode("v=1&dataX=1"), Left(Vector(FilterError.UnknownParameter("dataX"))))
    assertEquals(FilterQuery.decode("v=1&extension.a=1"), Left(Vector(FilterError.UnknownParameter("extension.a"))))

  test("a comparison value no smart constructor would accept never reaches the AST"):
    // `1E+2000000000` is a valid BigDecimal with one significant digit. Rendering it as a jsonpath literal allocates
    // two gigabytes, and the only thing standing between a permalink and that allocation is `NumLit`.
    assert(FilterQuery.decode("v=1&data.t=%3E1E%2B2000000000").isLeft)
    assert(FilterQuery.decode(s"v=1&data.t=%3E${"1" * (NumLit.MaxPrecision + 1)}").isLeft)
    assert(FilterQuery.decode(s"v=1&ext.tenantid=${"a" * (ExtValue.MaxLength + 1)}").isLeft)
