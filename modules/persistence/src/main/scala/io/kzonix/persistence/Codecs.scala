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
import com.augustnagro.magnum.pg.PgCodec
import com.augustnagro.magnum.pg.SqlArrayCodec
import io.circe.Json
import io.circe.parser
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLDataException
import java.sql.Types

/** JDBC codecs for the two Postgres types the domain refuses to know about.
  *
  * The point of both is negative: **no `org.postgresql.util.PGobject` and no `java.sql.Array` ever escapes this
  * module.** A repository returns `io.circe.Json`, which is the same type `modules/kernel` already speaks, so the
  * jsonb-ness of the storage stops at the edge of `persistence` and swapping Magnum out (ADR §12.4 budgets for it) does
  * not ripple into the domain.
  */
object Codecs:

  /** `jsonb` ⇄ circe `Json`.
    *
    * Written as `setObject(pos, text, Types.OTHER)` rather than by constructing a `PGobject`: with `OTHER` the driver
    * sends the value as an *unknown*-typed literal, which Postgres then coerces to whatever the target column or cast
    * requires. That keeps the codec driver-shaped rather than pgjdbc-class-shaped, and it is why callers can write
    * `?::jsonb` for a comparison and a bare `?` for an insert into a jsonb column.
    *
    * Reading goes through `getString`, so what comes back is Postgres's *canonical* jsonb rendering — key order,
    * insignificant whitespace and duplicate keys are already gone by then. That is a property of the storage decision
    * (ADR §12.4), not of this codec: `payload_sha256` exists precisely because jsonb cannot promise byte-identity.
    */
  given jsonb: DbCodec[Json] with

    def queryRepr: String = "?"

    def cols: IArray[Int] = IArray(Types.OTHER)

    def readSingle(rs: ResultSet, pos: Int): Json =
      val text = rs.getString(pos)
      if text == null then Json.Null
      else
        parser
          .parse(text)
          .fold(
            failure => throw SQLDataException(s"column $pos is not valid JSON: ${failure.message}"),
            identity
          )

    def writeSingle(value: Json, ps: PreparedStatement, pos: Int): Unit =
      ps.setObject(pos, value.noSpaces, Types.OTHER)

  /** `text[]`, for the `= ANY(?)` and `tags @> ?` filter cases.
    *
    * Summoned explicitly rather than imported as a given. `-Wunused:all` + `-Werror` has known false positives on
    * given-imports that are only consumed inside Magnum's `sql` macro (ADR §12.4), and naming the instances here means
    * there is no import to be wrongly flagged and no ambiguity about which array element type is in play.
    *
    * Callers must cast the placeholder — `?::text[]` — because the driver names the element type `VARCHAR`, and
    * `text[] @> varchar[]` is not a resolvable operator. The cast also pins one plan-cache entry for a filter whose
    * list length varies, which is the whole reason ADR §6.2 binds these lists as a single array parameter instead of an
    * `IN (?, ?, …)`.
    */
  val textArray: DbCodec[Vector[String]] =
    PgCodec.VectorCodec[String](using DbCodec.StringCodec, SqlArrayCodec.StringSqlArrayCodec)
