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

package com.worxbend.persistence.search

import com.worxbend.kernel.search.Filter
import com.worxbend.kernel.search.NumOp
import com.worxbend.persistence.Filters
import com.worxbend.persistence.Filters.force
import com.worxbend.persistence.Recording
import io.circe.Json

/** Adversarial tests for the one property the module cannot get wrong.
  *
  * The shape of every test is the same and is stronger than "the SQL looks safe": **the compiled statement text for a
  * hostile value is byte-identical to the text for a harmless one.** If a value could influence the SQL at all — by
  * escaping, by truncation, by normalisation, by encoding — the two texts would differ. Checking for the absence of a
  * quote character would only prove that today's payload was handled; equality proves that *no* payload is handled,
  * because the value never reaches the text at all.
  *
  * The second half of each test is the complement: the value did go somewhere, and that somewhere is a JDBC bind
  * parameter observed at the `PreparedStatement` boundary. A compiler that dropped hostile input on the floor would
  * pass the first assertion and fail this one.
  */
final class FilterSqlInjectionSuite extends munit.FunSuite:

  private val benign = "benign"

  private def sqlOf(filter: Filter): String = FilterSql.compile(filter).sqlString

  private def boundValues(filter: Filter): Vector[Any] =
    Recording.bind(FilterSql.compile(filter))._2.all

  private def eachHostile(name: String)(build: String => Filter): Unit =
    val reference = sqlOf(build(benign))
    Filters.hostile.foreach: payload =>
      assertEquals(
        sqlOf(build(payload)),
        reference,
        s"$name: SQL text changed for payload ${payload.map(c => f"\\u${c.toInt}%04x").mkString}"
      )

  test("a type filter's SQL is independent of its values, and the values are bound"):
    eachHostile("TypeIn")(payload => force(Filter.typeIn(Vector(payload))))
    Filters.hostile.foreach: payload =>
      assert(boundValues(force(Filter.typeIn(Vector(payload)))).contains(payload), payload)

  test("source, device, room and person filters behave identically"):
    eachHostile("SourceIn")(payload => force(Filter.sourceIn(Vector(payload))))
    eachHostile("DeviceIn")(payload => force(Filter.deviceIn(Vector(payload))))
    eachHostile("RoomIn")(payload => force(Filter.roomIn(Vector(payload))))
    eachHostile("PersonIn")(payload => force(Filter.personIn(Vector(payload))))
    Filters.hostile.foreach: payload =>
      assert(boundValues(force(Filter.deviceIn(Vector(payload)))).contains(payload), payload)

  test("a list of hostile values is one array parameter, not one placeholder each"):
    // The plan-cache argument of ADR §6.2 and the injection argument are the same mechanism here: if the list were
    // spliced into an IN-list, both would be lost at once.
    val frag = FilterSql.compile(force(Filter.typeIn(Filters.hostile)))
    assertEquals(frag.sqlString, "(ce_type = ANY(?::text[]))")
    assertEquals(frag.params.size, 1)

  test("free-text search is independent of the query text"):
    eachHostile("FullText")(payload => force(Filter.fullText(payload)))
    Filters.hostile.foreach: payload =>
      // UserText trims, so compare against what the kernel actually stored.
      assert(boundValues(force(Filter.fullText(payload))).contains(payload.trim), payload)

  test("an extension filter's value is independent of the SQL"):
    eachHostile("ExtensionEq")(payload => force(Filter.extensionEq("tenantid", payload)))
    Filters.hostile.foreach: payload =>
      // The name is bound twice — once for the indexable `??` existence conjunct, once for the `->>` value test — and
      // the value once. Both conjuncts must bind, or one of them is reading a literal that is not there.
      val values = boundValues(force(Filter.extensionEq("tenantid", payload)))
      assertEquals(values, Vector[Any]("tenantid", "tenantid", payload))

  test("an extension filter's NAME is independent of the SQL too, and reaches both conjuncts"):
    // The name is the half that also appears in an operator position in the shape this leaf compiles to
    // (`extensions ?? ?`), so it gets its own adversarial pass rather than riding on the value's.
    val names = Vector("tenantid", "sequence", "traceparent", "a", "z9", "0", "abcdefghijklmnopqrst")
    val reference = sqlOf(force(Filter.extensionEq("tenantid", benign)))
    names.foreach: name =>
      assertEquals(sqlOf(force(Filter.extensionEq(name, benign))), reference, s"SQL text changed for name '$name'")
      assertEquals(boundValues(force(Filter.extensionEq(name, benign))), Vector[Any](name, name, benign))
    // `ExtName` rejects most hostile payloads outright — but not all of them, and the exception is the point:
    // `0x27204f5220313d31` is lowercase alphanumeric and 18 characters, so it is a perfectly legal CloudEvents
    // extension name. A test that asserted "every hostile name is rejected" would have been asserting the wrong
    // thing and would fail on the one payload that gets through. The property that holds for both halves is that the
    // SQL does not move.
    Filters.hostile.foreach: payload =>
      Filter.extensionEq(payload, benign) match
        case Left(_)       => ()
        case Right(filter) =>
          assertEquals(sqlOf(filter), reference, s"SQL text changed for name '$payload'")
          assertEquals(boundValues(filter), Vector[Any](payload, payload, benign))

  test("a payload comparison's path, operator and value are all independent of the SQL"):
    // Every part of a `PayloadCmp` is rendered into a jsonpath *string* by the kernel, and the whole string is one
    // bind parameter. So the compiled text must not move when the path, the operator or the number changes — if it
    // did, the jsonpath would be reaching the statement by concatenation and `JsonPath`'s segment pattern would be
    // the only thing standing between a filter bar and the parser.
    val reference = sqlOf(force(Filter.payloadCmp("value", NumOp.Eq, BigDecimal(1))))
    val paths = Vector("value", "temperature", "sensor.temperature", "a.b.c.d.e.f.g.h")
    val numbers = Vector(BigDecimal(0), BigDecimal(-1), BigDecimal("21.5"), BigDecimal("-0.000000000000000001"))
    for path <- paths; op <- NumOp.values.toVector; number <- numbers do
      val frag = FilterSql.compile(force(Filter.payloadCmp(path, op, number)))
      assertEquals(frag.sqlString, reference, s"SQL text changed for $path ${op.symbol} $number")
      assertEquals(frag.params.size, 1)
      // The one parameter is the jsonpath expression, and it carries no quote for a jsonpath parser to reopen.
      val rendered = frag.params.head.toString
      assert(rendered.startsWith("$."), rendered)
      assert(!rendered.contains('\''), rendered)
      assert(!rendered.contains('"'), rendered)
    // A hostile path never becomes a filter at all: `JsonPath` rejects every segment that is not an identifier.
    Filters.hostile.foreach: payload =>
      assert(Filter.payloadCmp(payload, NumOp.Gt, BigDecimal(1)).isLeft, s"JsonPath admitted '$payload'")

  test("a payload comparison value cannot be inflated into a statement-sized literal"):
    // `toPlainString` renders `precision + |scale|` characters and `scale` comes straight off a permalink. Without
    // `NumLit`'s bound, `?v=1&data.t=>1E%2B2000000000` allocates two gigabytes of jsonpath before any of it reaches
    // the driver — a denial of service with no quote in it, which is why the injection suite is where it is caught.
    assert(Filter.payloadCmp("t", NumOp.Gt, BigDecimal("1E+2000000000")).isLeft)
    assert(Filter.payloadCmp("t", NumOp.Gt, BigDecimal("1E-2000000000")).isLeft)
    val widest = force(Filter.payloadCmp("t", NumOp.Gt, BigDecimal("1E-18")))
    assertEquals(FilterSql.compile(widest).params.head.toString.length, 32)

  test("a payload containment filter is one jsonb parameter regardless of its keys and values"):
    eachHostile("PayloadContains")(payload => force(Filter.payloadContains(Json.obj(payload -> Json.True))))
    eachHostile("PayloadContains")(payload =>
      force(Filter.payloadContains(Json.obj("k" -> Json.fromString(payload))))
    )
    Filters.hostile.foreach: payload =>
      val frag = FilterSql.compile(force(Filter.payloadContains(Json.obj("k" -> Json.fromString(payload)))))
      assertEquals(frag.params.size, 1)
      // The parameter is the JSON document, escaped by circe — which is JSON escaping, not SQL escaping, and it
      // happens entirely inside the value.
      assert(frag.params.head.toString.startsWith("{"), frag.params.head.toString)

  test("nesting hostile leaves inside boolean nodes does not change the SQL either"):
    def build(payload: String): Filter =
      force(
        Filter.or(
          Vector(
            force(Filter.typeIn(Vector(payload))),
            Filter.not(force(Filter.deviceIn(Vector(payload)))),
            force(Filter.and(Vector(force(Filter.fullText(payload)), force(Filter.sourceIn(Vector(payload))))))
          )
        )
      )
    eachHostile("nested")(build)

  test("the classic payload cannot reach the statement text through any leaf"):
    val classic = "'; DROP TABLE events.cloud_event; --"
    val filters = Vector(
      force(Filter.typeIn(Vector(classic))),
      force(Filter.sourceIn(Vector(classic))),
      force(Filter.deviceIn(Vector(classic))),
      force(Filter.roomIn(Vector(classic))),
      force(Filter.personIn(Vector(classic))),
      force(Filter.extensionEq("tenantid", classic)),
      force(Filter.fullText(classic)),
      force(Filter.payloadContains(Json.obj("k" -> Json.fromString(classic))))
    )
    filters.foreach: filter =>
      val sql = sqlOf(filter)
      assert(!sql.contains("DROP"), sql)
      assert(!sql.contains(';'), sql)
      assert(!sql.contains('\''), sql)
      assert(!sql.contains("--"), sql)
