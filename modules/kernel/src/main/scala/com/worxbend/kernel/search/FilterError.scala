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

/** Why a permalink could not be read, or a filter could not be written as one.
  *
  * Parsing a query string is total (ADR §6.3): a bad parameter is surfaced in the filter bar next to the value the user
  * typed, never a 500 and never a silently ignored filter — the second is worse than the first, because the user then
  * trusts a result set that was never filtered the way the URL says.
  */
enum FilterError:

  /** A parameter this version of the codec does not know. Reported rather than dropped, so a truncated or hand-mangled
    * link cannot masquerade as a narrower search than it is.
    */
  case UnknownParameter(name: String)

  /** `v` is absent. The version is mandatory precisely so that a future format change is detectable. */
  case MissingVersion

  case UnsupportedVersion(found: String)

  /** A single-valued parameter appeared more than once; which one wins would be arbitrary. */
  case Repeated(name: String)

  /** The value failed its smart constructor; `reason` is that constructor's message. */
  case Invalid(name: String, reason: String)

  /** The percent-encoding itself is broken, so there is no key/value pair to attribute the failure to. */
  case Malformed(fragment: String, reason: String)

  /** The filter is not expressible as a flat query string — see [[FilterQuery.encode]]. */
  case NotPermalinkable(reason: String)

  /** Rendered next to the offending input in the filter bar. */
  def message: String = this match
    case UnknownParameter(name)    => s"unknown parameter '$name'"
    case MissingVersion            => "missing 'v' parameter"
    case UnsupportedVersion(found) => s"unsupported permalink version '$found'"
    case Repeated(name)            => s"parameter '$name' may appear only once"
    case Invalid(name, reason)     => s"parameter '$name': $reason"
    case Malformed(fragment, why)  => s"malformed query fragment '$fragment': $why"
    case NotPermalinkable(reason)  => s"filter cannot be written as a permalink: $reason"
