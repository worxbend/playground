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

import com.worxbend.kernel.search.Filter
import com.worxbend.kernel.search.NumOp
import com.worxbend.kernel.search.Severity
import io.circe.Json
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import org.scalacheck.Gen

/** Generators for the filter grammar, tuned for the compiler rather than for the permalink codec.
  *
  * Two differences from the kernel's own generators, both deliberate:
  *
  *   - **arbitrary boolean nesting.** The permalink can only express a flat conjunction, so the kernel has no reason to
  *     generate `Or(Not(And(…)))`. The compiler does: parameter ordering under `++` is only interesting when fragments
  *     nest, and a writer that composes wrongly is invisible at depth one.
  *   - **hostile leaf values.** Every string leaf draws from a pool that includes quotes, comment introducers,
  *     statement terminators, dollar quotes and unicode escape syntax. A property that only ever sees `identifier12`
  *     proves nothing about escaping.
  */
object Filters:

  def force[A](result: Either[String, A]): A =
    result.fold(message => throw IllegalArgumentException(message), identity)

  /** Built rather than written as an escape sequence: a literal NUL is the character most easily lost in a copy or a
    * reformat, and it is exactly the byte a naive escaper silently drops.
    */
  private val embeddedNul: String = String(Array('a', 0.toChar, 'b'))

  /** Inputs shaped like every classic injection, plus the ones people forget.
    *
    * These are not expected to be *escaped*. They are expected to be *bound* — the assertion everywhere is that the
    * compiled SQL is byte-identical to the SQL for a harmless value, so nothing here can influence statement text.
    */
  val hostile: Vector[String] = Vector(
    "'; DROP TABLE events.cloud_event; --",
    "\" OR 1=1 --",
    "' OR '1'='1",
    "\\'; SELECT pg_sleep(10); --",
    "/* block comment */ UNION ALL SELECT NULL",
    "$$ dollar quoted $$",
    "$tag$ nested $tag$",
    "1; COMMIT; DROP SCHEMA events CASCADE",
    "U&'\\0027 OR 1=1'",
    "E'\\\\x27'",
    "%27%20OR%201%3D1",
    "0x27204f5220313d31",
    // A fullwidth apostrophe: harmless until something normalises it back into a real one.
    "＇ OR 1=1",
    embeddedNul,
    // A line comment that only bites once a newline puts it at the start of a line.
    "line\nbreak -- tail",
    "?", // the placeholder character itself
    "??", // pgjdbc's escape for it
    "@?", // the jsonb operator that contains one
    // Astral plane, to catch anything doing arithmetic on chars rather than code points.
    "💣 boom"
  )

  private val genHostile: Gen[String] = Gen.oneOf(hostile)

  private val genValue: Gen[String] =
    Gen.frequency(
      3 -> Gen.identifier.map(_.take(12)),
      1 -> Gen.const("com.worxbend.iot.telemetry"),
      1 -> Gen.const("/gateways/1"),
      3 -> genHostile
    )

  private val genValues: Gen[Vector[String]] =
    Gen.choose(1, 4).flatMap(n => Gen.listOfN(n, genValue)).map(_.toVector)

  private val genTime: Gen[OffsetDateTime] =
    for
      epochSecond <- Gen.choose(0L, 4102444800L)
      quarters <- Gen.choose(-56, 56)
    yield OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), ZoneOffset.ofTotalSeconds(quarters * 900))

  private val genOccurred: Gen[Filter] =
    Gen.oneOf(
      genTime.map(t => force(Filter.occurred(Some(t), None))),
      genTime.map(t => force(Filter.occurred(None, Some(t)))),
      for
        from <- genTime
        span <- Gen.choose(1L, 1000000L)
      yield force(Filter.occurred(Some(from), Some(from.plusSeconds(span))))
    )

  private val genLeaf: Gen[Filter] =
    Gen.oneOf(
      genOccurred,
      genValues.map(vs => force(Filter.typeIn(vs))),
      genValues.map(vs => force(Filter.sourceIn(vs))),
      genValues.map(vs => force(Filter.deviceIn(vs))),
      genValues.map(vs => force(Filter.roomIn(vs))),
      genValues.map(vs => force(Filter.personIn(vs))),
      Gen.oneOf(Severity.values.toIndexedSeq).map(Filter.severityAtLeast),
      Gen
        .choose(1, 3)
        .flatMap(n => Gen.listOfN(n, Gen.oneOf("indoor", "floor-1", "hvac:zone", "a.b", "x+y")))
        .map(ts => force(Filter.tagsAll(ts))),
      for
        key <- Gen.oneOf(Gen.identifier.map(_.take(8)), genHostile)
        value <- Gen.oneOf(Gen.const(Json.True), genHostile.map(Json.fromString), Gen.const(Json.fromInt(3)))
      yield force(Filter.payloadContains(Json.obj(key -> value))),
      for
        segments <- Gen.choose(1, 3).flatMap(n => Gen.listOfN(n, Gen.oneOf("temperature", "value", "level")))
        op <- Gen.oneOf(NumOp.values.toIndexedSeq)
        unscaled <- Gen.choose(-100000L, 100000L)
        scale <- Gen.choose(0, 3)
      yield force(Filter.payloadCmp(segments.mkString("."), op, BigDecimal(unscaled, scale))),
      for
        name <- Gen.oneOf("tenantid", "sequence", "traceparent")
        value <- genValue
      yield force(Filter.extensionEq(name, value)),
      genHostile.map(text => force(Filter.fullText(text)))
    )

  /** Boolean trees up to the given depth. Nesting is what exercises writer composition. */
  def genFilter(depth: Int): Gen[Filter] =
    if depth <= 0 then genLeaf
    else
      Gen.frequency(
        4 -> genLeaf,
        2 -> branches(depth).map(fs => force(Filter.and(fs))),
        2 -> branches(depth).map(fs => force(Filter.or(fs))),
        1 -> genFilter(depth - 1).map(Filter.not)
      )

  /** The default shape: deep enough that a broken `++` shows up, shallow enough to stay fast. */
  val genNested: Gen[Filter] = genFilter(3)

  private def branches(depth: Int): Gen[Vector[Filter]] =
    Gen.choose(2, 4).flatMap(n => Gen.listOfN(n, genFilter(depth - 1))).map(_.toVector.distinct)
