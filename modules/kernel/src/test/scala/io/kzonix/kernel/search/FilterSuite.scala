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

package io.kzonix.kernel.search

import java.time.OffsetDateTime

import io.circe.Json
import io.circe.parser
import org.scalacheck.Prop.forAll

/** The smart constructors.
  *
  * Each rejection below corresponds to a filter that would compile to valid SQL and return a wrong answer silently —
  * an empty `= ANY('{}')`, an inverted `BETWEEN`, an unbounded `WHERE` that prunes no partitions. Those are the ones
  * worth making unrepresentable; a syntax error would have announced itself.
  */
final class FilterSuite extends munit.ScalaCheckSuite:

  private val t0 = OffsetDateTime.parse("2026-07-01T00:00:00Z")
  private val t1 = OffsetDateTime.parse("2026-08-01T00:00:00Z")

  test("a time range with no bounds is rejected"):
    assert(Filter.occurred(None, None).isLeft)

  test("an inverted or empty time range is rejected"):
    assert(Filter.occurred(Some(t1), Some(t0)).isLeft)
    assert(Filter.occurred(Some(t0), Some(t0)).isLeft)

  test("a half-open time range is accepted"):
    assert(Filter.occurred(Some(t0), None).isRight)
    assert(Filter.occurred(None, Some(t1)).isRight)
    assert(Filter.occurred(Some(t0), Some(t1)).isRight)

  test("empty and blank value lists are rejected"):
    assert(Filter.typeIn(Nil).isLeft)
    assert(Filter.deviceIn(Vector("a", " ")).isLeft)
    assert(Filter.roomIn(Vector("")).isLeft)

  test("value lists are deduplicated and sorted so the AST is canonical"):
    assertEquals(Filter.typeIn(Vector("b", "a", "b")), Filter.typeIn(Vector("a", "b")))
    Filter.typeIn(Vector("b", "a", "b")) match
      case Right(Filter.TypeIn(vs)) => assertEquals(vs: Vector[String], Vector("a", "b"))
      case other                    => fail(s"expected a TypeIn, got $other")

  test("tags are validated element by element"):
    assert(Filter.tagsAll(Nil).isLeft)
    assert(Filter.tagsAll(Vector("ok", "not ok")).isLeft)
    assert(Filter.tagsAll(Vector("ok", "floor-1", "hvac:zone")).isRight)

  test("extension names must obey the CloudEvents rule and carry a value"):
    assert(Filter.extensionEq("tenantid", "acme").isRight)
    assert(Filter.extensionEq("TenantId", "acme").isLeft)
    assert(Filter.extensionEq("tenant-id", "acme").isLeft)
    assert(Filter.extensionEq("thisnameiswaytoolongforacloudevent", "acme").isLeft)
    assert(Filter.extensionEq("tenantid", "").isLeft)

  test("payload paths reject anything that is not an identifier segment"):
    assert(Filter.payloadCmp("temperature", NumOp.Gt, BigDecimal(21)).isRight)
    assert(Filter.payloadCmp("sensor.temperature", NumOp.Gt, BigDecimal(21)).isRight)
    assert(Filter.payloadCmp("", NumOp.Gt, BigDecimal(21)).isLeft)
    assert(Filter.payloadCmp("a b", NumOp.Gt, BigDecimal(21)).isLeft)
    assert(Filter.payloadCmp("1st", NumOp.Gt, BigDecimal(21)).isLeft)
    assert(Filter.payloadCmp("a'; DROP TABLE events.cloud_event; --", NumOp.Gt, BigDecimal(21)).isLeft)
    assert(Filter.payloadCmp("a.b.c.d.e.f.g.h.i", NumOp.Gt, BigDecimal(21)).isLeft)

  test("a validated path renders a jsonpath expression with nothing to escape"):
    val path = FilterGenerators.force(JsonPath.parse("sensor.temperature"))
    assertEquals(path.jsonPath, "$.sensor.temperature")
    assertEquals(path.jsonPathPredicate(NumOp.Gte, BigDecimal("21.5")), "$.sensor.temperature ? (@ >= 21.5)")
    assert(!path.jsonPathPredicate(NumOp.Gt, BigDecimal(1)).contains("'"))

  test("payload containment must be a bounded JSON object"):
    assert(Filter.payloadContains(Json.obj("room" -> Json.fromString("kitchen"))).isRight)
    assert(Filter.payloadContains(Json.fromString("kitchen")).isLeft)
    assert(Filter.payloadContains(Json.arr(Json.fromInt(1))).isLeft)
    val deep = (0 until 12).foldLeft(Json.obj())((acc, _) => Json.obj("n" -> acc))
    assert(Filter.payloadContains(deep).isLeft)

  test("free text is trimmed, must be non-blank, and is length-capped"):
    assert(Filter.fullText("   ").isLeft)
    assert(Filter.fullText("a" * (UserText.MaxLength + 1)).isLeft)
    assertEquals(Filter.fullText("  kitchen  "), Filter.fullText("kitchen"))

  test("operators the user would actually type are accepted"):
    assertEquals(NumOp.parse(">="), Some(NumOp.Gte))
    assertEquals(NumOp.parse("="), Some(NumOp.Eq))
    assertEquals(NumOp.parse("<>"), Some(NumOp.Ne))
    assertEquals(NumOp.parse("=>"), None)

  test("severity accepts the aliases the SQL rank function folds"):
    assertEquals(Severity.parse("WARNING"), Right(Severity.Warn))
    assertEquals(Severity.parse("emergency"), Right(Severity.Fatal))
    assertEquals(Severity.rank("crit"), Some(60))
    assertEquals(Severity.rank("nonsense"), None)
    assert(Severity.parse("nonsense").isLeft)

  test("severity ranks match the SQL function in ADR §5"):
    val ddl = Map(
      "debug"    -> 10,
      "info"     -> 20,
      "notice"   -> 30,
      "warn"     -> 40,
      "warning"  -> 40,
      "error"    -> 50,
      "critical" -> 60,
      "alert"    -> 70,
      "fatal"    -> 80,
      "emergency" -> 80
    )
    ddl.foreach((label, rank) => assertEquals(Severity.rank(label), Some(rank), s"rank of '$label'"))

  test("boolean nodes collapse their degenerate cases"):
    val leaf = Filter.severityAtLeast(Severity.Warn)
    assert(Filter.and(Nil).isLeft)
    assert(Filter.or(Nil).isLeft)
    assertEquals(Filter.and(Vector(leaf)), Right(leaf))
    assertEquals(Filter.or(Vector(leaf)), Right(leaf))
    assertEquals(Filter.and(Vector(leaf, leaf)), Right(leaf))

  test("nested conjunctions are flattened so one filter has one representation"):
    val a = FilterGenerators.force(Filter.typeIn(Vector("a")))
    val b = FilterGenerators.force(Filter.deviceIn(Vector("b")))
    val c = Filter.severityAtLeast(Severity.Error)
    val nested = Filter.and(Vector(FilterGenerators.force(Filter.and(Vector(a, b))), c))
    assertEquals(nested, Filter.and(Vector(c, b, a)))

  test("double negation is eliminated"):
    val leaf = Filter.severityAtLeast(Severity.Warn)
    assertEquals(Filter.not(Filter.not(leaf)), leaf)
    assertEquals(Filter.not(leaf), Filter.Not(leaf))

  property("conjunction is commutative in its representation, not merely in its meaning"):
    forAll(FilterGenerators.genFlatFilter): filter =>
      val leaves = Filter.leaves(filter)
      Filter.and(leaves.reverse) == Filter.and(leaves)

  /** `And`, `Or` and `Not` are ordinals 0-2; every leaf case follows them, which is also what makes `sortKey` produce
    * the permalink parameter order of ADR §6.3.
    */
  property("leaves of a flat filter contain no boolean nodes"):
    forAll(FilterGenerators.genFlatFilter): filter =>
      Filter.leaves(filter).forall(leaf => leaf.ordinal >= 3)

  test("JsonLit parsing rejects text that is not JSON"):
    assert(JsonLit.parse("{oops").isLeft)
    assert(JsonLit.parse("""{"a":1}""").isRight)
    assertEquals(JsonLit.parse("""{"a":1}""").map(_.noSpaces), Right("""{"a":1}"""))
    assert(parser.parse("""{"a":1}""").toOption.flatMap(j => JsonLit(j).toOption).isDefined)
