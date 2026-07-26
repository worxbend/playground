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

package io.kzonix.persistence

import io.circe.Json
import java.sql.Types

/** The two JDBC codecs.
  *
  * The assertion that matters in both cases is negative: no `PGobject` and no `java.sql.Array` leaves the module, and a
  * jsonb column read back is an `io.circe.Json` the domain already knows how to handle.
  */
final class CodecsSuite extends munit.FunSuite:

  test("jsonb is written as an unknown-typed value, so Postgres coerces it at the target"):
    // Types.OTHER rather than a PGobject: the driver sends the value untyped and the column, or an explicit ::jsonb
    // cast, decides. That is what lets one codec serve both `INSERT ... VALUES (?)` and `data @> ?::jsonb`.
    assertEquals(Codecs.jsonb.cols.toVector, Vector(Types.OTHER))

  test("a jsonb parameter is the compact rendering, and only ever one parameter"):
    val json = Json.obj("device" -> Json.fromString("kitchen-1"), "value" -> Json.fromInt(21))
    val frag = Sql.bind(json)(using Codecs.jsonb)
    assertEquals(frag.sqlString, "?")
    val (next, bindings) = Recording.bind(frag)
    assertEquals(next, 2)
    assertEquals(bindings.values, Vector[Any](json.noSpaces))

  test("reading jsonb yields circe Json, and a NULL column is Json.Null rather than an exception"):
    val rs = Recording.resultSet(Map(1 -> """{"a":[1,2],"b":null}"""))
    assertEquals(Codecs.jsonb.readSingle(rs, 1), io.circe.parser.parse("""{"a":[1,2],"b":null}""").toOption.get)
    assertEquals(Codecs.jsonb.readSingle(Recording.resultSet(Map.empty), 1), Json.Null)

  test("a column that is not JSON fails loudly rather than being swallowed"):
    val rs = Recording.resultSet(Map(1 -> "not json"))
    intercept[java.sql.SQLDataException](Codecs.jsonb.readSingle(rs, 1))

  test("a text array is one parameter, and its elements cross the boundary as elements"):
    // One bind slot for the whole list is the plan-cache decision of ADR §6.2; it is also what keeps a 300-value
    // filter from being 300 placeholders spliced into the statement.
    val values = Vector("kitchen-1", "'; DROP TABLE x; --", "hall")
    val frag = Sql.bind(values)(using Codecs.textArray)
    assertEquals(frag.sqlString, "?")
    assertEquals(Codecs.textArray.cols.length, 1)
    val (next, bindings) = Recording.bind(frag)
    assertEquals(next, 2)
    assertEquals(bindings.arrayElements, values.map(v => v: Any))
