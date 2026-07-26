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

package io.kzonix.kernel.event

import java.net.URI

/** A semantic version, used to key the [[Observation]] decoder registry.
  *
  * Only `major` participates in dispatch (ADR §4.2): minor and patch bumps are required to be additive, so a decoder
  * registered for major 1 must keep working against 1.7.3. Keeping the full triple anyway means a stored event can
  * still be explained precisely years later.
  */
final case class SemVer(major: Int, minor: Int, patch: Int) extends Ordered[SemVer]:

  def render: String = s"$major.$minor.$patch"

  override def compare(that: SemVer): Int =
    val byMajor = major.compare(that.major)
    if byMajor != 0 then byMajor
    else
      val byMinor = minor.compare(that.minor)
      if byMinor != 0 then byMinor else patch.compare(that.patch)

object SemVer:

  private val Pattern = """^(\d{1,9})\.(\d{1,9})\.(\d{1,9})$""".r

  /** Deliberately not a full SemVer 2.0.0 parser: pre-release and build metadata have no meaning for a schema registry
    * keyed on major, and accepting them would invite `1.2.0-rc1` and `1.2.0` to be treated as the same schema.
    */
  def parse(raw: String): Option[SemVer] = raw match
    case Pattern(major, minor, patch) => Some(SemVer(major.toInt, minor.toInt, patch.toInt))
    case _                            => None

/** The CloudEvents `dataschema` attribute.
  *
  * ADR §4.1 declares `SchemaRef(uri, name, version)` with `name` and `version` required. That cannot be honoured
  * literally without losing data: `dataschema` is any URI, and one that does not end in `…/<name>/<major.minor.patch>`
  * would have nowhere to go. Since ADR §4.2 also requires the raw URI to be stored verbatim, the URI is made the single
  * field and `name`/`version` become derived views. Equality is therefore on the URI alone, which is what makes the
  * envelope round-trip exact.
  */
final case class SchemaRef(uri: URI):

  private val segments: Vector[String] =
    Option(uri.getPath)
      .filter(_.nonEmpty)
      .orElse(Option(uri.getSchemeSpecificPart))
      .getOrElse("")
      .split('/')
      .iterator
      .filter(_.nonEmpty)
      .toVector

  /** The version encoded in the last path segment, when there is one. */
  val version: Option[SemVer] = segments.lastOption.flatMap(SemVer.parse)

  /** The schema name — the segment immediately before the version. Absent whenever the version is. */
  val name: Option[String] =
    if version.isDefined && segments.sizeIs >= 2 then Some(segments(segments.size - 2)) else None

  /** The registry dispatch key half. See [[Observation.from]]. */
  def major: Option[Int] = version.map(_.major)

object SchemaRef:

  def parse(raw: String): Either[String, SchemaRef] =
    Attr.uriReference("dataschema", raw).map(s => SchemaRef(URI(s)))
