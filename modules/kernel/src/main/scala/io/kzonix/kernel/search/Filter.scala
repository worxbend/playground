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

import java.time.OffsetDateTime

import io.circe.Json

/** Branches of a boolean node: at least two, because [[Filter.and]] and [[Filter.or]] collapse the degenerate cases.
  *
  * `And(Vector(f))` and `And(Vector.empty)` are not merely redundant, they are two different spellings of things the
  * rest of the pipeline has to special-case: the empty conjunction is "match everything" written as if it were a
  * filter, and the singleton is a second representation of `f` that hashes differently. Forbidding both keeps the AST
  * canonical, which is what makes the content-hashed saved search of ADR §6.3 well defined.
  */
opaque type Branches <: Vector[Filter] = Vector[Filter]

object Branches:

  def of(branches: Vector[Filter]): Either[String, Branches] =
    if branches.sizeIs < 2 then Left("a boolean node needs at least two branches") else Right(branches)

/** The search filter grammar (ADR §6.1).
  *
  * **No case carries a raw SQL string, and none ever will.** Every leaf holds a value type whose smart constructor has
  * already validated it, so the compiler in `modules/persistence` is a total function from this ADT to a parameterised
  * fragment and has no decision to make about escaping. The compiler is deliberately *not* here: the kernel must stay
  * free of a database.
  *
  * Case order is load-bearing. `ordinal` is the sort key the querystring codec canonicalises on, and it was chosen to
  * produce the parameter order ADR §6.3 shows: time, then dimensions, then severity, tags, payload, extensions, text.
  */
enum Filter:
  case And(fs: Branches)
  case Or(fs: Branches)
  case Not(f: Filter)
  case Occurred(from: Option[OffsetDateTime], until: Option[OffsetDateTime])
  case TypeIn(vs: Values)
  case SourceIn(vs: Values)
  case DeviceIn(vs: Values)
  case RoomIn(vs: Values)
  case PersonIn(vs: Values)
  case SeverityAtLeast(level: Severity)
  case TagsAll(vs: Tags)
  case PayloadContains(json: JsonLit)
  case PayloadCmp(path: JsonPath, op: NumOp, value: BigDecimal)
  case ExtensionEq(name: ExtName, value: String)
  case FullText(text: UserText)

object Filter:

  /** Conjunction, normalised: nested `And`s are flattened, duplicates removed, branches sorted, and a single survivor
    * returned unwrapped.
    *
    * Normalising in the constructor rather than in a later pass means there is exactly one representation of a given
    * filter, so equality, the permalink and the content hash all agree. Reordering is safe because `AND` is
    * commutative — the SQL compiler emits one fragment per branch and the planner reorders anyway.
    */
  def and(branches: Iterable[Filter]): Either[String, Filter] =
    normalise(branches, { case And(fs) => fs }) match
      case Vector()       => Left("and requires at least one filter")
      case Vector(single) => Right(single)
      case many           => Branches.of(many).map(fs => And(fs))

  /** Disjunction, normalised the same way. */
  def or(branches: Iterable[Filter]): Either[String, Filter] =
    normalise(branches, { case Or(fs) => fs }) match
      case Vector()       => Left("or requires at least one filter")
      case Vector(single) => Right(single)
      case many           => Branches.of(many).map(fs => Or(fs))

  /** Double negation is eliminated so `not(not(f))` is `f` rather than a second encoding of it. */
  def not(filter: Filter): Filter = filter match
    case Not(inner) => inner
    case other      => Not(other)

  /** A time window with neither bound is not a filter, it is the absence of one — and it would compile to a `WHERE`
    * clause that prunes no partitions while looking like it does. An inverted or empty window is always a mistake.
    */
  def occurred(from: Option[OffsetDateTime], until: Option[OffsetDateTime]): Either[String, Filter] =
    (from, until) match
      case (None, None) => Left("a time range needs at least one bound")
      case (Some(f), Some(u)) if !f.isBefore(u) =>
        Left(s"time range is empty: from '$f' is not before until '$u'")
      case _ => Right(Occurred(from, until))

  def typeIn(values: Iterable[String]): Either[String, Filter]   = Values.of(values).map(TypeIn.apply)
  def sourceIn(values: Iterable[String]): Either[String, Filter] = Values.of(values).map(SourceIn.apply)
  def deviceIn(values: Iterable[String]): Either[String, Filter] = Values.of(values).map(DeviceIn.apply)
  def roomIn(values: Iterable[String]): Either[String, Filter]   = Values.of(values).map(RoomIn.apply)
  def personIn(values: Iterable[String]): Either[String, Filter] = Values.of(values).map(PersonIn.apply)

  def severityAtLeast(level: Severity): Filter = SeverityAtLeast(level)

  def tagsAll(values: Iterable[String]): Either[String, Filter] = Tags.parse(values).map(TagsAll.apply)

  def payloadContains(json: Json): Either[String, Filter] = JsonLit(json).map(PayloadContains.apply)

  def payloadCmp(path: String, op: NumOp, value: BigDecimal): Either[String, Filter] =
    JsonPath.parse(path).map(p => PayloadCmp(p, op, value))

  def extensionEq(name: String, value: String): Either[String, Filter] =
    for
      n <- ExtName(name)
      v <- if value.isEmpty then Left("an extension filter needs a value") else Right(value)
    yield ExtensionEq(n, v)

  def fullText(text: String): Either[String, Filter] = UserText(text).map(FullText.apply)

  /** The canonical order of branches, and of a permalink's parameters.
    *
    * `ordinal` alone is not a total order — several `PayloadCmp`s share it — so `toString` breaks ties. It is a blunt
    * key, but it is total, deterministic and stable across JVMs, which is all a canonical form needs.
    */
  def sortKey(filter: Filter): (Int, String) = (filter.ordinal, filter.toString)

  /** Flattens a top-level conjunction into its leaves. A non-`And` filter is its own single leaf. */
  def leaves(filter: Filter): Vector[Filter] = filter match
    case And(fs) => fs.flatMap(leaves)
    case other   => Vector(other)

  private def normalise(branches: Iterable[Filter], unwrap: PartialFunction[Filter, Vector[Filter]]): Vector[Filter] =
    branches.toVector
      .flatMap(f => unwrap.applyOrElse(f, (other: Filter) => Vector(other)))
      .distinct
      .sortBy(sortKey)
