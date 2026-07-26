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

import io.circe.Json
import io.circe.parser

/** A filter tag. Tags are a `text[]` column indexed with `array_ops` (index (9) in ADR §5), so containment is the only
  * operation — there is no substring path — and the character set is restricted accordingly.
  */
opaque type Tag <: String = String

object Tag:

  private val Pattern = "^[A-Za-z0-9][A-Za-z0-9._:+-]{0,63}$".r

  def apply(raw: String): Either[String, Tag] =
    if Pattern.matches(raw) then Right(raw) else Left(s"'$raw' is not a tag")

/** A CloudEvents extension attribute name.
  *
  * The spec's rule (lowercase alphanumeric, 1–20 characters) is enforced here rather than treated as advice, because
  * this value reaches SQL as `extensions ->> ?`. It is a bind parameter, not an identifier, so the restriction is not
  * what makes it safe — but a filter on a name no conformant producer could emit is a silent empty result, and
  * rejecting it in the filter bar is the honest outcome.
  */
opaque type ExtName <: String = String

object ExtName:

  private val Pattern = "^[a-z0-9]{1,20}$".r

  def apply(raw: String): Either[String, ExtName] =
    if Pattern.matches(raw) then Right(raw)
    else Left(s"'$raw' is not a CloudEvents extension name (lowercase alphanumeric, 1-20 characters)")

/** Free text destined for `websearch_to_tsquery`.
  *
  * Only length- and blank-checked: `websearch_to_tsquery` — unlike `to_tsquery` — never raises on malformed input, so
  * sanitising the query would remove operators the user meant (`"quoted phrase" -excluded or`) and buy nothing. The cap
  * exists so a pathological paste cannot become a pathological tsquery.
  */
opaque type UserText <: String = String

object UserText:

  val MaxLength: Int = 512

  def apply(raw: String): Either[String, UserText] =
    val trimmed = raw.trim
    if trimmed.isEmpty then Left("search text must not be blank")
    else if trimmed.length > MaxLength then Left(s"search text must be at most $MaxLength characters")
    else Right(trimmed)

/** A path into the `data` object, as validated segments.
  *
  * Opaque with no `String` bound on purpose — the only sanctioned uses are the accessors below. `render` produces the
  * permalink form; `jsonPath` produces a jsonpath *expression* that the compiler in `modules/persistence` binds as a
  * `::jsonpath` parameter (ADR §6.1). The segment pattern is what makes the rendered expression safe to build by
  * concatenation: no quote, backslash or bracket can appear in a segment, so no escaping is required and none can be
  * got wrong.
  */
opaque type JsonPath = Vector[String]

object JsonPath:

  val MaxSegments: Int = 8

  private val Segment = "^[A-Za-z_][A-Za-z0-9_]{0,62}$".r

  /** Dot-separated, as it appears in a permalink: `data.sensor.temperature` arrives here as `sensor.temperature`. */
  def parse(raw: String): Either[String, JsonPath] = of(raw.split('.').toVector)

  def of(segments: Vector[String]): Either[String, JsonPath] =
    if segments.isEmpty then Left("a payload path needs at least one segment")
    else if segments.sizeIs > MaxSegments then Left(s"a payload path may have at most $MaxSegments segments")
    else
      segments.find(s => !Segment.matches(s)) match
        case Some(bad) => Left(s"'$bad' is not a payload path segment")
        case None      => Right(segments)

  extension (path: JsonPath)

    def segments: Vector[String] = path

    /** The permalink form. */
    def render: String = path.mkString(".")

    /** A jsonpath expression rooted at `data`, for `data @? ?::jsonpath`. */
    def jsonPath: String = path.mkString("$.", ".", "")

    /** The full existence predicate the `PayloadCmp` case compiles to. Rendered here rather than in `persistence`
      * because it is jsonpath, not SQL, and the segment validation that makes it safe lives here too.
      */
    def jsonPathPredicate(op: NumOp, value: BigDecimal): String =
      s"${path.jsonPath} ? (@ ${op.symbol} ${value.bigDecimal.toPlainString})"

/** A JSON literal for `data @> ?::jsonb` containment.
  *
  * Restricted to objects: containment against a scalar or array at the top level does not mean what a user typing into
  * a filter bar expects, and index (7) — `jsonb_path_ops` — is built for the object form. The depth cap bounds the work
  * the planner does per row and, just as importantly, bounds the recursion in this module's own validation.
  */
opaque type JsonLit <: Json = Json

object JsonLit:

  val MaxDepth: Int = 8

  def apply(json: Json): Either[String, JsonLit] =
    if !json.isObject then Left("a payload containment filter must be a JSON object")
    else if depthOf(json, 0) > MaxDepth then Left(s"a payload containment filter may nest at most $MaxDepth deep")
    else Right(json)

  def parse(raw: String): Either[String, JsonLit] =
    parser.parse(raw).left.map(failure => s"not JSON: ${failure.message}").flatMap(apply)

  private def depthOf(json: Json, soFar: Int): Int =
    if soFar > MaxDepth then soFar
    else
      json.fold(
        jsonNull = soFar,
        jsonBoolean = _ => soFar,
        jsonNumber = _ => soFar,
        jsonString = _ => soFar,
        jsonArray = values => values.foldLeft(soFar)((acc, v) => acc.max(depthOf(v, soFar + 1))),
        jsonObject = obj => obj.values.foldLeft(soFar)((acc, v) => acc.max(depthOf(v, soFar + 1)))
      )

/** A non-empty, deduplicated, sorted set of strings for the `col = ANY(?)` cases.
  *
  * Non-emptiness is the invariant that matters: `TypeIn(Vector.empty)` compiles to `ce_type = ANY('{}')`, which matches
  * nothing, so an empty selection would silently return zero rows instead of being reported to the user. Sorting makes
  * the AST — and therefore its permalink and its content hash (ADR §6.3) — canonical.
  */
opaque type Values <: Vector[String] = Vector[String]

object Values:

  def of(raw: Iterable[String]): Either[String, Values] =
    val vs = raw.toVector
    if vs.isEmpty then Left("at least one value is required")
    else if vs.exists(_.isBlank) then Left("values must not be blank")
    else Right(vs.distinct.sorted)

/** The [[Tag]] equivalent of [[Values]], for `tags @> ?`. */
opaque type Tags <: Vector[Tag] = Vector[Tag]

object Tags:

  def of(raw: Iterable[Tag]): Either[String, Tags] =
    val vs = raw.toVector
    if vs.isEmpty then Left("at least one tag is required") else Right(vs.distinct.sorted)

  /** Validates each element, reporting the first offender rather than silently dropping it. */
  def parse(raw: Iterable[String]): Either[String, Tags] =
    val collected = raw.foldLeft[Either[String, Vector[Tag]]](Right(Vector.empty)): (acc, candidate) =>
      acc.flatMap(tags => Tag(candidate).map(tags :+ _))
    collected.flatMap(of)

/** Numeric comparison operators for [[Filter.PayloadCmp]].
  *
  * `symbol` is the *jsonpath* spelling, not the SQL one — equality is `==` in jsonpath — because that is where these
  * end up. The permalink codec reuses the same spelling so a hand-edited URL and the compiled predicate agree.
  */
enum NumOp(val symbol: String):
  case Lt extends NumOp("<")
  case Lte extends NumOp("<=")
  case Gt extends NumOp(">")
  case Gte extends NumOp(">=")
  case Eq extends NumOp("==")
  case Ne extends NumOp("!=")

object NumOp:

  /** Two-character operators are tried first; `=` and `<>` are accepted as the spellings a human would actually type
    * into a URL bar.
    */
  def parse(raw: String): Option[NumOp] = raw match
    case ">=" => Some(Gte)
    case "<=" => Some(Lte)
    case "==" => Some(Eq)
    case "!=" => Some(Ne)
    case "<>" => Some(Ne)
    case ">"  => Some(Gt)
    case "<"  => Some(Lt)
    case "="  => Some(Eq)
    case _    => None
