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

/** Ordered severity levels.
  *
  * `rank` is not decorative: text alone cannot be range-compared, so "at least warning" has to become
  * `severity_rank >= 40`. The numbers here are the *same* numbers as `events.severity_rank()` in ADR §5 — if the two
  * drift, the UI's alert filter and the database's partial index (11) silently disagree about what an alert is. Gaps of
  * ten leave room to insert a level without renumbering stored data.
  */
enum Severity(val label: String, val rank: Int) extends Ordered[Severity]:
  case Debug extends Severity("debug", 10)
  case Info extends Severity("info", 20)
  case Notice extends Severity("notice", 30)
  case Warn extends Severity("warn", 40)
  case Error extends Severity("error", 50)
  case Critical extends Severity("critical", 60)
  case Alert extends Severity("alert", 70)
  case Fatal extends Severity("fatal", 80)

  override def compare(that: Severity): Int = rank.compare(that.rank)

object Severity:

  /** Spellings the SQL function also folds. Producers are not consistent about `warn`/`warning`, and syslog-derived
    * gateways emit `emerg`/`crit`; accepting them here is what keeps ranking identical on both sides of the wire.
    */
  private val Aliases: Map[String, Severity] = Map(
    "warning" -> Warn,
    "err" -> Error,
    "crit" -> Critical,
    "emerg" -> Fatal,
    "emergency" -> Fatal,
    "panic" -> Fatal
  )

  private val ByLabel: Map[String, Severity] = values.iterator.map(s => s.label -> s).toMap ++ Aliases

  /** Every spelling [[parse]] accepts, canonical labels and aliases alike.
    *
    * Public so the integration tier can assert `events.severity_rank()` against all of them rather than against the
    * eight canonical labels only — the aliases are exactly the half that drifted, and iterating `values` cannot see it.
    */
  val Spellings: Set[String] = ByLabel.keySet

  /** The lowest severity that counts as an alert — index (11) in ADR §5 is partial on exactly this threshold. */
  val AlertThreshold: Severity = Error

  def parse(raw: String): Either[String, Severity] =
    ByLabel.get(raw.trim.toLowerCase) match
      case Some(severity) => Right(severity)
      case None           => Left(s"unknown severity '$raw'; expected one of ${values.map(_.label).mkString(", ")}")

  /** The numeric rank of an arbitrary producer-supplied string, or `None` when it is not a severity at all — the exact
    * contract of `events.severity_rank()`, which returns NULL rather than raising.
    */
  def rank(raw: String): Option[Int] = parse(raw).toOption.map(_.rank)
