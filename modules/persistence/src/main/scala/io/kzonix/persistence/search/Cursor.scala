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

import io.kzonix.kernel.Rfc3339
import io.kzonix.kernel.search.Filter
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.time.OffsetDateTime
import java.util.Base64
import java.util.UUID
import scala.util.Try

/** Which end of the timeline a page starts from.
  *
  * Only two directions exist, and both sort on `(occurred_at, event_uid)`. `event_uid` is not decoration: it is the
  * total-order tiebreaker without which two events sharing a timestamp — which happens constantly, because producers
  * batch — can be returned twice or skipped entirely as the page boundary moves. Both columns are `NOT NULL`, which
  * sidesteps the `NULLS FIRST`/`NULLS LAST` keyset trap (ADR §6.3).
  */
enum SortDirection:
  case Newest, Oldest

/** A fingerprint of the query a cursor belongs to.
  *
  * A keyset cursor is only meaningful relative to the `WHERE` clause and `ORDER BY` that produced it. Replaying one
  * against a different filter does not fail — it silently returns a plausible-looking page from the middle of an
  * unrelated result set, which is the worst possible failure mode because nothing looks wrong. Carrying the fingerprint
  * inside the cursor turns that into an explicit rejection.
  *
  * **This is integrity against mistakes, not against an adversary.** There is no key, so a determined caller can mint a
  * cursor for any filter. That is acceptable because the cursor's only effect is to supply two *bind parameters* to a
  * query the caller could already run: the worst a forged cursor achieves is a different page of the caller's own
  * results. Making it a MAC would imply a secret to rotate and distribute, for no reduction in blast radius.
  */
opaque type Fingerprint <: String = String

object Fingerprint:

  /** 12 hex characters — 48 bits. Cursors are compared for equality, never searched for, so this only has to make an
    * accidental collision between two filters a user might plausibly switch between vanishingly unlikely, while staying
    * short enough that the encoded cursor is a reasonable URL component.
    */
  val Length: Int = 12

  private val Pattern = "^[0-9a-f]{12}$".r

  /** The filter's `toString` is the canonical form.
    *
    * That is not laziness: `Filter`'s smart constructors normalise (flatten, deduplicate, sort) precisely so that one
    * filter has one representation, and `Filter.sortKey` already leans on `toString` for the same reason. Two ASTs that
    * mean the same thing therefore fingerprint alike, and two that differ do not.
    */
  def of(filter: Option[Filter], sort: SortDirection): Fingerprint =
    val canonical = s"${sort.toString} ${filter.fold("*")(_.toString)}"
    val digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(UTF_8))
    digest.iterator.map(b => f"${b & 0xff}%02x").mkString.take(Length)

  /** The way back in from a decoded cursor. Shape-checked rather than trusted, so a hand-mangled cursor fails here
    * rather than becoming a fingerprint that can never match anything.
    */
  def parse(raw: String): Either[String, Fingerprint] =
    if Pattern.matches(raw) then Right(raw) else Left(s"'$raw' is not a filter fingerprint")

/** Why a cursor was refused. */
enum CursorError(val message: String):

  /** Not something this build produced: bad base64, wrong field count, unknown version, unparseable timestamp. */
  case Malformed(reason: String) extends CursorError(s"cursor is malformed: $reason")

  /** Well-formed, but minted for a different query. The correct UI response is to start from the first page, not to
    * show an error — the filter changing under a user is normal, not exceptional.
    */
  case FilterChanged extends CursorError("cursor belongs to a different filter; start from the first page")

/** An opaque keyset cursor: the sort key of the last row of the previous page, plus the query it came from.
  *
  * Opaque *and* stable is the point. ADR §6.3 keeps permalinks readable and hand-editable so users can share and edit
  * them, and explicitly exempts cursors from that rule: a cursor is a position in a result set, not a description of
  * one, and inviting people to hand-edit it invites exactly the mismatch [[Fingerprint]] exists to catch.
  *
  * `OFFSET` is not acceptable at any page depth (ADR §6.3): it makes the database walk and discard every skipped row,
  * so page 10 000 costs 10 000 pages of work, and it double-counts or skips rows whenever an insert lands above the
  * window — which, for an append-heavy event store, is every request.
  *
  * The encoded form carries no checksum on purpose: the fields are fixed in number and each is self-validating, so a
  * corrupted cursor fails on the field that was corrupted instead of passing a checksum and failing later.
  */
final case class Cursor(occurredAt: OffsetDateTime, eventUid: UUID, fingerprint: Fingerprint):

  /** base64url without padding, so the value is a single URL component needing no percent-encoding. */
  def encode: String =
    val payload = Vector(Cursor.Version, Rfc3339.render(occurredAt), eventUid.toString, fingerprint)
      .mkString(Cursor.Separator)
    Base64.getUrlEncoder.withoutPadding.encodeToString(payload.getBytes(UTF_8))

object Cursor:

  /** Bumped when the field layout changes. An old cursor then fails as [[CursorError.Malformed]] rather than being
    * reinterpreted under the new layout — an unversioned encoding is one that can never be evolved safely.
    */
  val Version: String = "1"

  /** Not a character any field can contain: the version is fixed, RFC 3339 and UUID grammars exclude it, and the
    * fingerprint is hex. So splitting is unambiguous without escaping.
    */
  private val Separator: String = "|"

  private val Fields: Int = 4

  /** Decodes and checks the cursor against the query it is about to be used with.
    *
    * `expected` is not optional and there is deliberately no `decode` without it: a decode that hands back a cursor the
    * caller must remember to validate is a decode that will eventually be used unvalidated.
    */
  def decode(encoded: String, expected: Fingerprint): Either[CursorError, Cursor] =
    for
      payload <- decodeBase64(encoded)
      fields <- split(payload)
      cursor <- parse(fields)
      checked <- if cursor.fingerprint == expected then Right(cursor) else Left(CursorError.FilterChanged)
    yield checked

  private def decodeBase64(encoded: String): Either[CursorError, String] =
    Try(String(Base64.getUrlDecoder.decode(encoded), UTF_8)).toEither.left
      .map(_ => CursorError.Malformed("not base64url"))

  private def split(payload: String): Either[CursorError, Vector[String]] =
    // -1 keeps trailing empty fields, so a truncated cursor fails the field count instead of quietly shrinking.
    val fields = payload.split("\\|", -1).toVector
    if fields.sizeIs == Fields then Right(fields)
    else Left(CursorError.Malformed(s"expected $Fields fields, found ${fields.size}"))

  private def parse(fields: Vector[String]): Either[CursorError, Cursor] =
    for
      _ <-
        if fields(0) == Version then Right(())
        else Left(CursorError.Malformed(s"unsupported version '${fields(0)}'"))
      occurredAt <- Rfc3339.parse(fields(1)).left.map(CursorError.Malformed.apply)
      eventUid <- uuid(fields(2))
      fingerprint <- Fingerprint.parse(fields(3)).left.map(CursorError.Malformed.apply)
    yield Cursor(occurredAt, eventUid, fingerprint)

  private def uuid(raw: String): Either[CursorError, UUID] =
    Try(UUID.fromString(raw)).toEither.left.map(_ => CursorError.Malformed(s"'$raw' is not a UUID"))
