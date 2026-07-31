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

package com.worxbend.persistence

import com.augustnagro.magnum.DbCodec
import com.augustnagro.magnum.Frag
import java.sql.PreparedStatement
import scala.compiletime.constValueOpt
import scala.compiletime.error

/** The fragment algebra every query in this module is built from — and the one place SQL text is produced.
  *
  * **The security property is structural, not a convention.** [[lit]] is an inline method that rejects, at compile
  * time, any argument that is not a literal constant: it asks `constValueOpt` for the argument's value and calls
  * `compiletime.error` when there is none. A `String` computed at runtime — which is what user input is — has no
  * constant type, so it cannot become SQL text. The set of characters that can ever reach a statement is therefore
  * fixed when this module compiles, and every value the user supplied travels through [[bind]] into a JDBC placeholder.
  *
  * That is stronger than ADR §6.2's "`lit` is private to `FilterSql.scala`", which is a file-scope rule a reviewer has
  * to notice being broken, and it is why the fragment builders live here rather than being duplicated per query: there
  * is exactly one function in the module whose argument becomes SQL, and the compiler guards it.
  *
  * A `String & Singleton` bound alone was tried first and is **not** sufficient: a stable `val` holding a request
  * parameter has a singleton type and would have been accepted. The bound is still present — it is what stops the
  * literal type being widened away before `constValueOpt` sees it — but the constant check is what enforces the rule.
  * The only remaining escape hatch is `asInstanceOf`, which the ADR already bans by review.
  *
  * `++` is associative in the parameter positions as well as in the text, which is what makes arbitrary AND/OR/NOT
  * nesting work. Magnum's `FragWriter.write(ps, pos): Int` returns the *next* free position, so composing two writers
  * is function composition on that position — no fragment needs to know its own offset, and a deeply nested filter
  * binds its parameters in exactly the order its text mentions them.
  */
object Sql:

  /** The identity of [[++]]: no text, no parameters, no position advance. */
  val empty: Frag = Frag("", Vector.empty, (_, pos) => pos)

  /** Literal SQL text.
    *
    * The `inline match` is the safety argument: `constValueOpt` reduces to `Some` only for an argument with a constant
    * type, so `lit(userSuppliedString)` is a compile error rather than an injection.
    */
  inline def lit[T <: String & Singleton](text: T): Frag =
    inline constValueOpt[T] match
      case Some(_) => Frag(text, Vector.empty, (_, pos) => pos)
      case None    =>
        error(
          "Sql.lit takes compile-time constants only. Bind the value with Sql.bind, which sends it to the database as a JDBC parameter instead of splicing it into the statement."
        )

  /** A single bind parameter. The value never appears in `sqlString`; it is written into the statement by the codec at
    * whatever position composition has assigned it.
    *
    * `params` is carried alongside purely so Magnum's `SqlLogger` can render the query — it is not what binds. That
    * separation is also what the parameter-count property test exploits: `params.size` and the number of placeholders
    * in the text are produced by independent code paths, so a mismatch between them is a real defect and not a
    * tautology.
    *
    * Single-column codecs only, checked rather than assumed. A codec spanning several columns (Magnum's tuple codecs
    * do) would write two values behind one placeholder and shift every later parameter by one — a corruption that
    * produces plausible wrong results rather than an error. There is no such codec in this module today; the guard is
    * here so that adding one fails loudly on the first call.
    */
  def bind[A](value: A)(using codec: DbCodec[A]): Frag =
    require(
      codec.cols.length == 1,
      s"Sql.bind is for single-column codecs; this one spans ${codec.cols.length} columns"
    )
    Frag("?", Vector(value), (ps, pos) => writeOne(codec, value, ps, pos))

  /** `prefix ? suffix` — the shape of nearly every leaf predicate, expressed once so the leaves cannot drift.
    *
    * The two text parameters are type parameters bounded by `Singleton` rather than plain `String`, so the literal type
    * survives the call and [[lit]]'s constant check still has something to check. That is also why the value's type
    * parameter sits in the middle: all three are always inferred, and a call site that needs to name `A` explicitly
    * writes the three fragments out instead.
    */
  inline def unary[P <: String & Singleton, A, S <: String & Singleton](prefix: P, value: A, suffix: S)(using
    DbCodec[A]
  ): Frag =
    lit(prefix) ++ bind(value) ++ lit(suffix)

  /** Wraps in parentheses. Always applied to composite nodes: without it `NOT a OR b` means something other than what
    * the AST says, and the bug only appears for filters a user actually nested.
    */
  def parens(inner: Frag): Frag = lit("(") ++ inner ++ lit(")")

  /** Joins with a literal separator. Empty input yields [[empty]] rather than a dangling separator. */
  inline def join[S <: String & Singleton](separator: S, parts: Seq[Frag]): Frag =
    val glue = lit(separator)
    parts match
      case Seq()     => empty
      case Seq(only) => only
      case many      => many.reduceLeft((acc, next) => acc ++ glue ++ next)

  extension (left: Frag)

    /** Concatenation: text, parameter list and writer all compose. */
    infix def ++(right: Frag): Frag =
      Frag(
        left.sqlString + right.sqlString,
        left.params ++ right.params,
        (ps, pos) => right.writer.write(ps, left.writer.write(ps, pos))
      )

  private def writeOne[A](codec: DbCodec[A], value: A, ps: PreparedStatement, pos: Int): Int =
    codec.writeSingle(value, ps, pos)
    pos + codec.cols.length
