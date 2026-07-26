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

import com.augustnagro.magnum.DbCodec
import io.kzonix.persistence.Sql.++
import scala.compiletime.testing.typeChecks

/** The fragment algebra.
  *
  * These tests exist because everything else in the module trusts three claims: that `lit` cannot be handed a runtime
  * string, that `++` composes writers so parameters land in text order at any nesting depth, and that the parameter
  * list and the placeholder count are produced independently. Nothing above this file re-checks any of them.
  */
final class SqlSuite extends munit.FunSuite:

  test("a runtime string cannot become SQL text, and a literal can"):
    // The negative case is the whole security argument, so it is asserted rather than described in a comment: a
    // `String` is not a `String & Singleton`, and no amount of user input can produce one.
    assert(
      !typeChecks("""
        val userInput: String = List("any", "thing").mkString
        io.kzonix.persistence.Sql.lit(userInput)
      """),
      "Sql.lit accepted a non-constant String — the injection guarantee is gone"
    )
    assert(typeChecks("""io.kzonix.persistence.Sql.lit("SELECT 1")"""))
    // A stable `val` holding user input has a singleton type. An earlier version of `lit` bounded its parameter by
    // `String & Singleton` and accepted exactly this, which is why the check is `constValueOpt` and not a type bound.
    assert(
      !typeChecks("""
        val stable: String = scala.util.Random.nextString(4)
        io.kzonix.persistence.Sql.lit(stable)
      """),
      "Sql.lit accepted a stable reference to a runtime String"
    )

  test("empty is the identity of concatenation"):
    val frag = Sql.lit("a") ++ Sql.bind(1)
    assertEquals((Sql.empty ++ frag).sqlString, frag.sqlString)
    assertEquals((frag ++ Sql.empty).sqlString, frag.sqlString)
    assertEquals((Sql.empty ++ frag).params.size, 1)

  test("concatenation is associative in text, parameters and writer positions"):
    val a = Sql.unary("a = ", "x", "")
    val b = Sql.unary("b = ", 2, "")
    val c = Sql.unary("c = ", "z", "")
    val left = (a ++ b) ++ c
    val right = a ++ (b ++ c)
    assertEquals(left.sqlString, right.sqlString)
    assertEquals(left.params.toVector, right.params.toVector)
    assertEquals(Recording.bind(left), Recording.bind(right))

  test("parameters are written in the order the text mentions them, however deeply nested"):
    val inner = Sql.parens(Sql.join(" AND ", Vector(Sql.unary("a = ", "1", ""), Sql.unary("b = ", "2", ""))))
    val outer = Sql.parens(Sql.join(" OR ", Vector(inner, Sql.unary("c = ", "3", ""))))
    val (next, bindings) = Recording.bind(outer)
    assertEquals(bindings.positions, Vector(1, 2, 3))
    assertEquals(bindings.values, Vector[Any]("1", "2", "3"))
    assertEquals(next, 4)

  test("join of nothing is empty, and join of one is that one"):
    assertEquals(Sql.join(" AND ", Vector.empty).sqlString, "")
    assertEquals(Sql.join(" AND ", Vector(Sql.lit("x"))).sqlString, "x")

  test("a multi-column codec is rejected rather than silently shifting every later parameter"):
    // One placeholder that writes two values would push every subsequent parameter one position along. The result is
    // not an error, it is a query that returns the wrong rows — so this has to fail at the call, not at the database.
    val pair: DbCodec[(String, String)] = DbCodec.Tuple2Codec(using DbCodec.StringCodec, DbCodec.StringCodec)
    val failure = intercept[IllegalArgumentException](Sql.bind(("l", "r"))(using pair))
    assert(failure.getMessage.contains("single-column"), failure.getMessage)
