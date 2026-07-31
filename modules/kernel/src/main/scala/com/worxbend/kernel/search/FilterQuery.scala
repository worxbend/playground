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

import com.worxbend.kernel.Rfc3339
import java.nio.charset.StandardCharsets.UTF_8
import java.time.OffsetDateTime
import scala.annotation.tailrec
import scala.util.Try

/** The permalink codec (ADR §6.3).
  *
  * A readable, hand-editable query string with an explicit version — deliberately **not** base64 JSON, which is opaque,
  * unbounded and an unversioned format nobody can ever evolve. The shape is
  * `?v=1&from=…&type=…&device=…&tag=…&severity=>=warn&data.temperature=>21&q=…`.
  *
  * Two asymmetries are intentional:
  *
  *   - **Decoding is total.** Every failure becomes a [[FilterError]] positioned on the parameter that caused it, so a
  *     mangled link renders a filter bar with one field flagged instead of a 500 — and, more importantly, never renders
  *     unflagged results the URL did not ask for.
  *   - **Encoding is partial.** The query string expresses a flat conjunction of leaves; `Or` and `Not` have no
  *     readable flat spelling and get a [[FilterError.NotPermalinkable]] instead of a lossy approximation. ADR §6.3
  *     routes those, and anything over ~1.5 KB, to a content-hashed saved search (`?s=…`) instead.
  *
  * Parameter order is the AST's canonical branch order, so the same filter always produces the same string — which is
  * what makes that content hash a stable key.
  *
  * ==Why payload and extension filters are spelled as key prefixes==
  *
  * The two open-ended dimensions — any path under `data`, any CloudEvents extension attribute — are the only ones whose
  * *parameter name* is chosen by the user rather than fixed by the grammar:
  *
  * {{{
  * data.sensor.temperature=>21     PayloadCmp(sensor.temperature, Gt, 21)
  * ext.tenantid=acme               ExtensionEq(tenantid, "acme")
  * }}}
  *
  * The alternative considered and rejected was a fixed, repeatable key with the whole predicate in the value —
  * `payload=sensor.temperature>21`, `ext=tenantid:acme`. It has one real advantage: a plain HTML form can post it from
  * a single `<input name="payload">`, whereas a prefixed key needs a name the browser cannot compose without
  * JavaScript. It was rejected anyway, for three reasons that outlast that convenience.
  *
  *   - **Two escaping problems instead of none.** With the value carrying `path`, `op` and `number`, the codec has to
  *     find the operator inside a string that may legitimately contain `>` or `:` (an extension value routinely does —
  *     `traceparent` is colon-delimited). The prefixed form puts the delimiter in the query string's own `=`, which is
  *     already unambiguous because [[Percent]] escapes every `=` inside a key or a value.
  *   - **Editing stays a URL edit.** Every other filter family in this grammar is `key=value`, and the UI's whole
  *     navigation model is "change one parameter" — `Query.remove(pairs, key, value)` removes a chip, a facet click
  *     toggles a pair. A predicate hidden inside a value would need its own parser in the presentation layer just to
  *     take one predicate off.
  *   - **It reads.** `?data.temperature=>21&ext.tenantid=acme` is legible in a chat message and correctable in a
  *     browser bar, which is the requirement the whole "not base64 JSON" decision exists to serve.
  *
  * The lost form-friendliness costs nothing here: neither dimension has a visible input in the filter bar (nor do
  * `type`, `device`, `room`, `person` or `tag`), they travel through a submit as hidden fields, and they are added and
  * removed through links.
  *
  * `data.` deliberately shares its prefix with the bare `data=` containment key, because they are the same dimension
  * asked two ways; `data` alone can never be mistaken for a path, since [[JsonPath]] requires at least one segment.
  */
object FilterQuery:

  /** Bumped only for a breaking change to the grammar; decoding an unknown version fails loudly rather than
    * misinterpreting parameters that happen to share a name.
    */
  val Version: String = "1"

  private val TypeKey = "type"
  private val SourceKey = "source"
  private val DeviceKey = "device"
  private val RoomKey = "room"
  private val PersonKey = "person"
  private val TagKey = "tag"
  private val FromKey = "from"
  private val UntilKey = "until"
  private val SeverityKey = "severity"
  private val DataKey = "data"
  private val TextKey = "q"
  private val VersionKey = "v"

  private val PathPrefix = "data."
  private val ExtensionPrefix = "ext."

  /** Parameters that carry at most one value; a repeat is an error rather than a silent last-wins. */
  private val SingleValued: Set[String] =
    Set(VersionKey, FromKey, UntilKey, SeverityKey, DataKey, TextKey)

  private val Known: Set[String] =
    SingleValued ++ Set(TypeKey, SourceKey, DeviceKey, RoomKey, PersonKey, TagKey)

  /** Renders a filter as a permalink query string, without the leading `?`. */
  def encode(filter: Option[Filter]): Either[FilterError, String] =
    val leaves = filter.fold(Vector.empty[Filter])(Filter.leaves).sortBy(Filter.sortKey)
    val collected = leaves.foldLeft[Either[FilterError, Vector[(String, String)]]](Right(Vector.empty)): (acc, leaf) =>
      acc.flatMap(params => paramsOf(leaf).map(params ++ _))
    collected.map: params =>
      ((VersionKey -> Version) +: params)
        .map((key, value) => s"${Percent.encode(key)}=${Percent.encode(value)}")
        .mkString("&")

  /** Parses a permalink. Accepts a leading `?`. `Right(None)` means "a valid link with no filters" — the landing page —
    * which is a different answer from an error and the UI treats it differently.
    */
  def decode(queryString: String): Either[Vector[FilterError], Option[Filter]] =
    val fragments = queryString.stripPrefix("?").split('&').iterator.filter(_.nonEmpty).toVector
    val split = fragments.map(splitPair)
    val malformed = split.collect { case Left(error) => error }
    val pairs = split.collect { case Right(pair) => pair }
    val (leafErrors, leaves) = buildLeaves(pairs)
    val errors = malformed ++ versionErrors(pairs) ++ leafErrors
    if errors.nonEmpty then Left(errors)
    else if leaves.isEmpty then Right(None)
    else Filter.and(leaves).left.map(reason => Vector(FilterError.NotPermalinkable(reason))).map(Some.apply)

  private def paramsOf(leaf: Filter): Either[FilterError, Vector[(String, String)]] = leaf match
    case Filter.And(_) | Filter.Or(_) | Filter.Not(_) =>
      Left(FilterError.NotPermalinkable("only a flat conjunction of leaves has a query-string form"))
    case Filter.Occurred(from, until) =>
      Right(
        from.map(t => FromKey -> Rfc3339.render(t)).toVector ++
          until.map(t => UntilKey -> Rfc3339.render(t)).toVector
      )
    case Filter.TypeIn(vs)               => Right(vs.map(v => TypeKey -> v))
    case Filter.SourceIn(vs)             => Right(vs.map(v => SourceKey -> v))
    case Filter.DeviceIn(vs)             => Right(vs.map(v => DeviceKey -> v))
    case Filter.RoomIn(vs)               => Right(vs.map(v => RoomKey -> v))
    case Filter.PersonIn(vs)             => Right(vs.map(v => PersonKey -> v))
    case Filter.SeverityAtLeast(l)       => Right(Vector(SeverityKey -> s">=${l.label}"))
    case Filter.TagsAll(vs)              => Right(vs.map(v => TagKey -> (v: String)))
    case Filter.PayloadContains(js)      => Right(Vector(DataKey -> js.noSpaces))
    case Filter.PayloadCmp(p, op, value) =>
      // `toPlainString`, never `BigDecimal#toString`: the latter switches to scientific notation past a certain
      // scale, so `1E+10` would appear in a link nobody typed that way, and `+` percent-encodes to `%2B` — three
      // characters of noise in the one place this grammar promises legibility. `NumLit` bounds the plain form's
      // length, which is what makes rendering it unconditionally safe.
      Right(Vector(s"$PathPrefix${p.render}" -> s"${op.symbol}${value.bigDecimal.toPlainString}"))
    case Filter.ExtensionEq(name, value) => Right(Vector(s"$ExtensionPrefix$name" -> (value: String)))
    case Filter.FullText(text)           => Right(Vector(TextKey -> (text: String)))

  private def splitPair(fragment: String): Either[FilterError, (String, String)] =
    val separator = fragment.indexOf('=')
    val (rawKey, rawValue) =
      if separator < 0 then (fragment, "") else (fragment.take(separator), fragment.drop(separator + 1))
    for
      key <- Percent.decode(rawKey).left.map(reason => FilterError.Malformed(fragment, reason))
      value <- Percent.decode(rawValue).left.map(reason => FilterError.Malformed(fragment, reason))
    yield (key, value)

  private def versionErrors(pairs: Vector[(String, String)]): Vector[FilterError] =
    pairs.collectFirst { case (VersionKey, value) => value } match
      case None          => Vector(FilterError.MissingVersion)
      case Some(Version) => Vector.empty
      case Some(other)   => Vector(FilterError.UnsupportedVersion(other))

  private def buildLeaves(pairs: Vector[(String, String)]): (Vector[FilterError], Vector[Filter]) =
    val byKey: Map[String, Vector[String]] = pairs.groupMap((key, _) => key)((_, value) => value)

    def one(key: String): Option[String] = byKey.get(key).flatMap(_.headOption)
    def all(key: String): Vector[String] = byKey.getOrElse(key, Vector.empty)

    def valuesLeaf(key: String, make: Iterable[String] => Either[String, Filter]): Vector[Result] =
      val values = all(key)
      if values.isEmpty then Vector.empty else Vector(make(values).left.map(FilterError.Invalid(key, _)))

    def prefixedLeaves(prefix: String)(make: (String, String) => Either[String, Filter]): Vector[Result] =
      byKey.iterator
        .filter((key, _) => key.startsWith(prefix))
        .toVector
        .sortBy((key, _) => key)
        .flatMap: (key, values) =>
          values.map(value => make(key.drop(prefix.length), value).left.map(FilterError.Invalid(key, _)))

    val unknown =
      byKey.keysIterator
        .filterNot(key => Known(key) || key.startsWith(PathPrefix) || key.startsWith(ExtensionPrefix))
        .toVector
        .sorted
        .map(FilterError.UnknownParameter.apply)

    // A repeated `ext.<name>` is reported for a reason the fixed single-valued keys do not share: the grammar has
    // only equality on an extension, so two values for one name conjoin into a predicate no row can satisfy. Building
    // it would hand back an empty page that looks like "nothing matched" rather than "this link contradicts itself".
    // A repeated `data.<path>` is *not* an error — `data.t=>18&data.t=<24` is how the grammar spells a range.
    val repeated =
      byKey.iterator
        .collect {
          case (key, values) if (SingleValued(key) || key.startsWith(ExtensionPrefix)) && values.sizeIs > 1 => key
        }
        .toVector
        .sorted
        .map(FilterError.Repeated.apply)

    val results: Vector[Result] =
      occurredLeaf(one(FromKey), one(UntilKey)) ++
        valuesLeaf(TypeKey, Filter.typeIn) ++
        valuesLeaf(SourceKey, Filter.sourceIn) ++
        valuesLeaf(DeviceKey, Filter.deviceIn) ++
        valuesLeaf(RoomKey, Filter.roomIn) ++
        valuesLeaf(PersonKey, Filter.personIn) ++
        one(SeverityKey).toVector.map(severityLeaf) ++
        valuesLeaf(TagKey, Filter.tagsAll) ++
        one(DataKey).toVector.map(containsLeaf) ++
        prefixedLeaves(PathPrefix)(payloadCmpLeaf) ++
        prefixedLeaves(ExtensionPrefix)(Filter.extensionEq) ++
        one(TextKey).toVector.map(textLeaf)

    val errors = unknown ++ repeated ++ results.collect { case Left(error) => error }
    (errors, results.collect { case Right(filter) => filter })

  private type Result = Either[FilterError, Filter]

  private def occurredLeaf(from: Option[String], until: Option[String]): Vector[Result] =
    (parseTime(FromKey, from), parseTime(UntilKey, until)) match
      case (Right(None), Right(None)) => Vector.empty
      case (Left(a), Left(b))         => Vector(Left(a), Left(b))
      case (Left(a), _)               => Vector(Left(a))
      case (_, Left(b))               => Vector(Left(b))
      case (Right(f), Right(u))       =>
        Vector(Filter.occurred(f, u).left.map(FilterError.Invalid(FromKey, _)))

  private def parseTime(key: String, raw: Option[String]): Either[FilterError, Option[OffsetDateTime]] =
    raw match
      case None       => Right(None)
      case Some(text) =>
        Rfc3339.parse(text).left.map(reason => FilterError.Invalid(key, reason)).map(Some.apply)

  /** `>=warn` is the spelling ADR §6.3 shows; a bare `warn` is accepted because it is what a human types and
    * `SeverityAtLeast` is the only severity predicate in the grammar, so there is nothing to be ambiguous about.
    */
  private def severityLeaf(raw: String): Result =
    Severity
      .parse(raw.stripPrefix(">=").trim)
      .map(Filter.severityAtLeast)
      .left
      .map(FilterError.Invalid(SeverityKey, _))

  private def containsLeaf(raw: String): Result =
    JsonLit.parse(raw).map(Filter.PayloadContains.apply).left.map(FilterError.Invalid(DataKey, _))

  private def textLeaf(raw: String): Result =
    Filter.fullText(raw).left.map(FilterError.Invalid(TextKey, _))

  private def payloadCmpLeaf(path: String, raw: String): Either[String, Filter] =
    val symbol = raw.takeWhile(c => "<>=!".contains(c))
    val number = raw.drop(symbol.length).trim
    for
      op <- (if symbol.isEmpty then Some(NumOp.Eq) else NumOp.parse(symbol))
        .toRight(s"'$symbol' is not a comparison operator")
      value <- Try(BigDecimal(number)).toEither.left.map(_ => s"'$number' is not a number")
      leaf <- Filter.payloadCmp(path, op, value)
    yield leaf

/** Percent-encoding for the permalink.
  *
  * Hand-rolled rather than `java.net.URLEncoder` for one reason: `URLEncoder` escapes `:` and `/`, which turns every
  * `source` and `dataschema` in a link into unreadable noise and defeats the "hand-editable" requirement. The safe set
  * below keeps those legible while escaping everything that could change how the string parses. `+` is always escaped
  * on the way out and always decoded as a space on the way in, which is what a human pasting a form-encoded URL expects
  * and still round-trips exactly.
  */
private object Percent:

  private val Safe: Set[Char] = (('A' to 'Z') ++ ('a' to 'z') ++ ('0' to '9')).toSet ++
    Set('-', '.', '_', '~', ':', '/', '@', '*')

  def encode(raw: String): String =
    val pieces = raw.getBytes(UTF_8).iterator.map { byte =>
      val unsigned = byte & 0xff
      if Safe(unsigned.toChar) then unsigned.toChar.toString else f"%%$unsigned%02X"
    }
    pieces.mkString

  def decode(raw: String): Either[String, String] =
    @tailrec def go(index: Int, acc: Vector[Byte]): Either[String, Vector[Byte]] =
      if index >= raw.length then Right(acc)
      else
        raw.charAt(index) match
          case '%' =>
            if index + 2 >= raw.length then Left(s"truncated percent escape at $index")
            else
              hexByte(raw.charAt(index + 1), raw.charAt(index + 2)) match
                case None       => Left(s"invalid percent escape '${raw.substring(index, index + 3)}'")
                case Some(byte) => go(index + 3, acc :+ byte)
          case '+' => go(index + 1, acc :+ ' '.toByte)
          case ch  => go(index + 1, acc ++ ch.toString.getBytes(UTF_8).toVector)

    go(0, Vector.empty).map(bytes => String(bytes.toArray, UTF_8))

  private def hexByte(high: Char, low: Char): Option[Byte] =
    val h = Character.digit(high, 16)
    val l = Character.digit(low, 16)
    if h < 0 || l < 0 then None else Some(((h << 4) | l).toByte)
