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
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import org.scalacheck.Gen

/** Generators for the filter grammar.
  *
  * `genFlatFilter` produces exactly what a permalink can express: a conjunction with at most one leaf per dimension,
  * plus any number of payload-comparison and extension leaves. Generating arbitrary boolean trees here would only
  * exercise the encoder's rejection path, which has its own test.
  */
object FilterGenerators:

  def force[A](result: Either[String, A]): A =
    result.fold(message => throw IllegalArgumentException(message), identity)

  private val genValue: Gen[String] =
    Gen.oneOf(
      Gen.identifier.map(_.take(12)),
      Gen.const("com.worxbend.iot.telemetry"),
      Gen.const("/gateways/1"),
      Gen.const("https://home.example/a b?c=d&e"),
      Gen.const("room:kitchen")
    )

  private val genValues: Gen[Vector[String]] =
    Gen.choose(1, 4).flatMap(n => Gen.listOfN(n, genValue)).map(_.toVector)

  private val genTag: Gen[String] =
    Gen.oneOf(Gen.identifier.map(_.take(12)), Gen.const("indoor"), Gen.const("floor-1"), Gen.const("hvac:zone"))

  private val genTags: Gen[Filter] =
    Gen.choose(1, 3).flatMap(n => Gen.listOfN(n, genTag)).map(ts => force(Filter.tagsAll(ts)))

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

  private val genContains: Gen[Filter] =
    for
      key <- Gen.identifier.map(_.take(8))
      value <- Gen.oneOf(Json.fromString("open"), Json.fromInt(3), Json.True)
    yield force(Filter.payloadContains(Json.obj(key -> value)))

  /** Numbers drawn right up to [[NumLit]]'s bounds, not just the well-behaved middle.
    *
    * The interesting values for a *codec* are the ones whose `toString` and `toPlainString` disagree — a negative scale
    * renders as `1E+8` one way and `100000000` the other — because the permalink commits to the plain form and the
    * round-trip property is what proves the two spellings name the same filter.
    */
  private val genNumber: Gen[BigDecimal] =
    for
      unscaled <- Gen.choose(-100000L, 100000L)
      scale <- Gen.choose(-NumLit.MaxScale, NumLit.MaxScale)
    yield BigDecimal(unscaled, scale)

  private val genCmp: Gen[Filter] =
    for
      segments <- Gen.choose(1, 3).flatMap(n => Gen.listOfN(n, Gen.oneOf("temperature", "value", "level", "power")))
      op <- Gen.oneOf(NumOp.values.toIndexedSeq)
      number <- genNumber
    yield force(Filter.payloadCmp(segments.mkString("."), op, number))

  /** Extension names are drawn from a pool with no repeats within one filter — see [[genFlatFilter]]. */
  private val genExtensionName: Gen[String] =
    Gen.oneOf("tenantid", "sequence", "traceparent", "partitionkey")

  private val genExtension: Gen[(String, String)] =
    for
      name <- genExtensionName
      value <- genValue
    yield (name, value)

  private val genText: Gen[Filter] =
    Gen
      .oneOf("kitchen", "\"smoke detected\" -drill", "temp or humidity", "a & b")
      .map(text => force(Filter.fullText(text)))

  /** At least one leaf, at most one per single-valued dimension. */
  val genFlatFilter: Gen[Filter] =
    for
      occurred <- Gen.option(genOccurred)
      types <- Gen.option(genValues.map(vs => force(Filter.typeIn(vs))))
      sources <- Gen.option(genValues.map(vs => force(Filter.sourceIn(vs))))
      devices <- Gen.option(genValues.map(vs => force(Filter.deviceIn(vs))))
      rooms <- Gen.option(genValues.map(vs => force(Filter.roomIn(vs))))
      persons <- Gen.option(genValues.map(vs => force(Filter.personIn(vs))))
      severity <- Gen.option(Gen.oneOf(Severity.values.toIndexedSeq).map(Filter.severityAtLeast))
      tags <- Gen.option(genTags)
      contains <- Gen.option(genContains)
      comparisons <- Gen.choose(0, 3).flatMap(n => Gen.listOfN(n, genCmp))
      extensionPairs <- Gen.choose(0, 3).flatMap(n => Gen.listOfN(n, genExtension))
      text <- Gen.option(genText)
    yield
      // One leaf per extension name. Two equalities on one name conjoin into a predicate nothing can satisfy, so the
      // codec reports it as a repeated parameter rather than encoding it — this generator produces permalinkable
      // filters, and that is not one. Payload comparisons have no such rule: `data.t=>18&data.t=<24` is a range.
      val extensions = extensionPairs
        .distinctBy((name, _) => name)
        .map((name, value) => force(Filter.extensionEq(name, value)))
      val leaves =
        Vector(occurred, types, sources, devices, rooms, persons, severity, tags, contains, text).flatten ++
          comparisons ++ extensions
      force(Filter.and(if leaves.isEmpty then Vector(Filter.severityAtLeast(Severity.Warn)) else leaves))
