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

package com.worxbend.persistence.search

import com.augustnagro.magnum.Frag
import com.worxbend.kernel.search.Filter
import com.worxbend.persistence.Codecs
import com.worxbend.persistence.Sql
import com.worxbend.persistence.Sql.++
import io.circe.Json

/** `Filter ⇒ Frag`: the compiler from the search grammar to a parameterised SQL predicate (ADR §6.2).
  *
  * **Total, and security-critical.** Every branch below is `Sql.lit(<a literal in this file>)` interleaved with
  * `Sql.bind(<a value from the AST>)`. Nothing else is possible: `Sql.lit` only accepts compile-time constants, so
  * there is no expression in this file — and none that could be added to it — that turns a user-supplied string into
  * statement text. A quote, a `--`, a `;`, a `'` or a 4 KB paste all end up as the contents of one JDBC placeholder,
  * where they are data.
  *
  * That leaves the *shape* of the SQL as the only thing this file decides, and every shape is chosen to hit a named
  * index from ADR §5. The mapping is a design commitment, not an implementation detail: changing `= ANY(?)` to
  * `IN (?, ?, …)` would still be correct and still be safe, and it would also quietly fork the plan cache per filter
  * length.
  *
  * Two spellings look odd and are deliberate:
  *
  *   - `websearch_to_tsquery(?::regconfig, ?)` binds the text-search configuration instead of writing a quoted
  *     `simple`. The configuration is ours, not the user's, so this buys no safety directly — what it buys is an
  *     invariant a test can assert without a carve-out: **compiled SQL contains no quote character at all.** An
  *     invariant with an exception is an invariant nobody checks.
  *   - `data @?? ?::jsonpath` is not a typo. `?` is the JDBC placeholder, so the jsonb `@?` operator collides with it;
  *     pgjdbc's documented escape is `??`, which its parser rewrites to a single `?` before the statement reaches the
  *     server (verified against `postgresql 42.7.13`). The alternative, `jsonb_path_exists(data, …)`, is a function
  *     rather than an operator and therefore **not** in the `jsonb_path_ops` operator class — it would silently drop
  *     index (7) from the plan. ADR §6.2's warning about the `?` operator and its `@?` mapping are reconciled here, not
  *     contradicted.
  */
object FilterSql:

  /** The text-search configuration for `search_doc`. `simple`, never `english`: the corpus is device ids, MQTT topics
    * and reverse-DNS type names, which English stemming mangles. Must equal the configuration the `search_doc`
    * generated column in `V1__events.sql` uses for weights A, B and D, or the query and the index disagree about what a
    * lexeme is.
    */
  val TextSearchConfig: String = "simple"

  /** Compiles a filter into a boolean expression over `events.cloud_event`.
    *
    * The result is a bare predicate with no `WHERE` — callers splice it into whatever surrounds it (see [[whereOf]]),
    * and the facet and histogram queries splice the *same* fragment into their candidate CTE, which is what makes a
    * facet count mean "refine within the current selection" rather than "count of something adjacent".
    */
  def compile(filter: Filter): Frag = filter match
    case Filter.And(fs) => Sql.parens(Sql.join(" AND ", fs.map(compile)))
    case Filter.Or(fs)  => Sql.parens(Sql.join(" OR ", fs.map(compile)))
    case Filter.Not(f)  => Sql.lit("(NOT ") ++ compile(f) ++ Sql.lit(")")

    // Partition pruning first: a bounded window is what keeps a query off the older months entirely. Half-open
    // (`>= from`, `< until`) so adjacent windows tile without double-counting an event on a boundary.
    case Filter.Occurred(from, until) =>
      val bounds = Vector(
        from.map(v => Sql.unary("occurred_at >= ", v, "")),
        until.map(v => Sql.unary("occurred_at < ", v, ""))
      ).flatten
      // The kernel rejects a boundless window, but this function is total over the ADT and `TRUE` is the honest
      // compilation of "no constraint" — better than a crash on a case the type system still admits.
      if bounds.isEmpty then Sql.lit("TRUE") else Sql.parens(Sql.join(" AND ", bounds))

    // One bind slot for the whole list, so a 3-value and a 300-value filter share a plan-cache entry (ADR §6.2).
    case Filter.TypeIn(vs)   => anyOf("(ce_type = ANY(", vs)
    case Filter.SourceIn(vs) => anyOf("(ce_source = ANY(", vs)
    case Filter.DeviceIn(vs) => anyOf("(device_id = ANY(", vs)
    case Filter.RoomIn(vs)   => anyOf("(room_id = ANY(", vs)
    case Filter.PersonIn(vs) => anyOf("(person_id = ANY(", vs)

    // Rank, not label: text cannot be range-compared. Index (11) is partial on exactly `rank >= 50`.
    case Filter.SeverityAtLeast(level) => Sql.unary("severity_rank >= ", level.rank, "")

    // Containment, so "all of these tags" is one operator against index (9).
    case Filter.TagsAll(vs) =>
      Sql.lit("tags @> ") ++ Sql.bind[Vector[String]](vs)(using Codecs.textArray) ++ Sql.lit("::text[]")

    case Filter.PayloadContains(json) =>
      Sql.lit("data @> ") ++ Sql.bind[Json](json)(using Codecs.jsonb) ++ Sql.lit("::jsonb")

    // The jsonpath expression is rendered by the kernel, whose segment pattern forbids every character that would
    // need escaping — and it is still bound, never concatenated, so even a future loosening of that pattern cannot
    // reach the statement text.
    case Filter.PayloadCmp(path, op, value) =>
      Sql.unary("data @?? ", path.jsonPathPredicate(op, value), "::jsonpath")

    // `extensions ->> ?`, never the `?` operator: it is indistinguishable from a placeholder (ADR §6.2).
    case Filter.ExtensionEq(name, value) =>
      Sql.lit("(extensions ->> ") ++ Sql.bind[String](name) ++ Sql.lit(" = ") ++ Sql.bind(value) ++ Sql.lit(")")

    // `websearch_to_tsquery`, never `to_tsquery`: it accepts quoted phrases and `-exclusions` and — decisively — does
    // not raise on malformed input, so a stray `&` typed by a user is an empty result set rather than a 500.
    case Filter.FullText(text) =>
      Sql.lit("search_doc @@ websearch_to_tsquery(") ++ Sql.bind(TextSearchConfig) ++
        Sql.lit("::regconfig, ") ++ Sql.bind[String](text) ++ Sql.lit(")")

  /** `WHERE <predicate>`, or `WHERE TRUE` when there is no filter.
    *
    * `TRUE` rather than an omitted clause: every caller appends further conjuncts (the keyset predicate, the facet
    * cap), and a fragment that is sometimes a clause and sometimes nothing forces each of them to branch. Postgres
    * discards the constant during planning.
    */
  def whereOf(filter: Option[Filter]): Frag =
    Sql.lit(" WHERE ") ++ filter.fold(Sql.lit("TRUE"))(compile)

  /** `col = ANY(?::text[])`, with the closing parenthesis of the caller's prefix.
    *
    * Inline with a `Singleton`-bounded prefix so the literal type survives into `Sql.lit`, which is what keeps the
    * constant check meaningful one call away from the literal.
    */
  private inline def anyOf[P <: String & Singleton](prefix: P, values: Vector[String]): Frag =
    Sql.lit(prefix) ++ Sql.bind[Vector[String]](values)(using Codecs.textArray) ++ Sql.lit("::text[]))")
