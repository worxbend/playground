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

package io.kzonix.persistence.search

import io.kzonix.kernel.search.Filter
import io.kzonix.persistence.Filters
import io.kzonix.persistence.Filters.force
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

/** The cursor codec and its fingerprint.
  *
  * The round trip is the obvious property. The interesting one is the rejection: a cursor replayed against a different
  * filter must be refused, because the failure it prevents is silent — the query succeeds, returns rows, and those rows
  * are a page of the wrong result set.
  */
final class CursorSuite extends munit.ScalaCheckSuite:

  private val genTime: Gen[OffsetDateTime] =
    for
      epochSecond <- Gen.choose(0L, 4102444800L)
      nanos <- Gen.choose(0, 999999999)
      quarters <- Gen.choose(-56, 56)
    yield OffsetDateTime
      .ofInstant(Instant.ofEpochSecond(epochSecond, nanos.toLong), ZoneOffset.ofTotalSeconds(quarters * 900))

  private val genUuid: Gen[UUID] =
    for
      high <- Gen.choose(Long.MinValue, Long.MaxValue)
      low <- Gen.choose(Long.MinValue, Long.MaxValue)
    yield UUID(high, low)

  private val genSort: Gen[SortDirection] = Gen.oneOf(SortDirection.values.toIndexedSeq)

  property("encoding then decoding a cursor is the identity"):
    forAll(genTime, genUuid, Filters.genNested, genSort): (time, uid, filter, sort) =>
      val fingerprint = Fingerprint.of(Some(filter), sort)
      val cursor = Cursor(time, uid, fingerprint)
      Cursor.decode(cursor.encode, fingerprint) == Right(cursor)

  property("an encoded cursor is a single URL component"):
    forAll(genTime, genUuid, Filters.genNested, genSort): (time, uid, filter, sort) =>
      // base64url without padding: nothing here needs percent-encoding, so a cursor survives being pasted into a
      // querystring, a shell and a log line unchanged.
      Cursor(time, uid, Fingerprint.of(Some(filter), sort)).encode.forall(c =>
        c.isLetterOrDigit && c < 128 || c == '-' || c == '_'
      )

  property("a cursor minted for one filter is refused by another"):
    forAll(genTime, genUuid, Filters.genNested, Filters.genNested): (time, uid, one, other) =>
      val a = Fingerprint.of(Some(one), SortDirection.Newest)
      val b = Fingerprint.of(Some(other), SortDirection.Newest)
      val encoded = Cursor(time, uid, a).encode
      a == b || Cursor.decode(encoded, b) == Left(CursorError.FilterChanged)

  property("decoding never throws, whatever the input"):
    forAll(Gen.asciiPrintableStr): raw =>
      scala.util.Try(Cursor.decode(raw, Fingerprint.of(None, SortDirection.Newest))).isSuccess

  test("changing only the sort direction invalidates the cursor"):
    val filter = force(Filter.typeIn(Vector("io.kzonix.iot.telemetry")))
    val newest = Fingerprint.of(Some(filter), SortDirection.Newest)
    val oldest = Fingerprint.of(Some(filter), SortDirection.Oldest)
    assertNotEquals(newest: String, oldest: String)
    val encoded = Cursor(OffsetDateTime.parse("2026-07-01T00:00:00Z"), UUID.randomUUID(), newest).encode
    assertEquals(Cursor.decode(encoded, oldest), Left(CursorError.FilterChanged))

  test("the absent filter has its own fingerprint, distinct from any real one"):
    val none = Fingerprint.of(None, SortDirection.Newest)
    val some = Fingerprint.of(Some(force(Filter.typeIn(Vector("a")))), SortDirection.Newest)
    assertNotEquals(none: String, some: String)

  test("filters that normalise to the same AST share a fingerprint"):
    // The kernel sorts and deduplicates branches, so two spellings of one conjunction are one filter. If the
    // fingerprint disagreed, reordering a filter bar's chips would throw the user back to page one.
    val a = force(Filter.and(Vector(
      force(Filter.typeIn(Vector("x"))),
      Filter.severityAtLeast(io.kzonix.kernel.search.Severity.Warn)
    )))
    val b = force(Filter.and(Vector(
      Filter.severityAtLeast(io.kzonix.kernel.search.Severity.Warn),
      force(Filter.typeIn(Vector("x")))
    )))
    assertEquals(
      Fingerprint.of(Some(a), SortDirection.Newest): String,
      Fingerprint.of(Some(b), SortDirection.Newest): String
    )

  test("malformed cursors are rejected with a reason, not an exception"):
    val fingerprint = Fingerprint.of(None, SortDirection.Newest)
    def malformed(encoded: String): Boolean =
      Cursor.decode(encoded, fingerprint) match
        case Left(CursorError.Malformed(_)) => true
        case _                              => false

    assert(malformed("not base64 at all !!!"))
    assert(malformed(encode("1|2026-07-01T00:00:00Z"))) // too few fields
    assert(malformed(encode("1|2026-07-01T00:00:00Z|" + UUID.randomUUID() + "|x|extra"))) // too many
    assert(malformed(encode("2|2026-07-01T00:00:00Z|" + UUID.randomUUID() + "|" + fingerprint)))
    assert(malformed(encode("1|not-a-time|" + UUID.randomUUID() + "|" + fingerprint)))
    assert(malformed(encode("1|2026-07-01T00:00:00Z|not-a-uuid|" + fingerprint)))
    assert(malformed(encode("1|2026-07-01T00:00:00Z|" + UUID.randomUUID() + "|ZZZZZZZZZZZZ")))

  test("a truncated cursor fails the field count rather than shrinking quietly"):
    val fingerprint = Fingerprint.of(None, SortDirection.Newest)
    val encoded = encode("1|2026-07-01T00:00:00Z|" + UUID.randomUUID() + "|")
    assert(Cursor.decode(encoded, fingerprint).isLeft)

  test("a fingerprint is stable across calls, so a cursor issued now still parses later"):
    val filter = force(Filter.typeIn(Vector("a")))
    assertEquals(
      Fingerprint.of(Some(filter), SortDirection.Newest): String,
      Fingerprint.of(Some(filter), SortDirection.Newest): String
    )
    assertEquals((Fingerprint.of(Some(filter), SortDirection.Newest): String).length, Fingerprint.Length)

  private def encode(payload: String): String =
    java.util.Base64.getUrlEncoder.withoutPadding
      .encodeToString(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8))
