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

package io.kzonix.ferrite

import io.kzonix.ferrite.wiring.AllowedHosts
import munit.FunSuite

/** The comma-separated `ALLOWED_HOSTS` string in, `AllowedHostsFilter`'s list out.
  *
  * This exists because the failure it guards is invisible until deployment. `play.filters.hosts.allowed` is typed as a
  * list, an environment substitution is always a string, and the wrong-type error therefore fires only when the
  * variable is actually set — never in development, always in production. Splitting the string ourselves moves the
  * whole question into a pure function, and this suite is what makes that function's edges deliberate rather than
  * incidental.
  */
final class AllowedHostsSuite extends FunSuite:

  test("a plain comma-separated list becomes one entry per host"):
    assertEquals(AllowedHosts.parse("localhost,127.0.0.1,.local"), Seq("localhost", "127.0.0.1", ".local"))

  test("surrounding whitespace is not part of a hostname"):
    // A `.env` written by a human has spaces after commas, and an untrimmed " 127.0.0.1" matches nothing at all —
    // which surfaces as a 400 on one hostname rather than as a configuration error.
    assertEquals(AllowedHosts.parse(" localhost , 127.0.0.1 "), Seq("localhost", "127.0.0.1"))

  test("empty entries are dropped rather than turned into a host that matches nothing"):
    assertEquals(AllowedHosts.parse("localhost,,127.0.0.1,"), Seq("localhost", "127.0.0.1"))

  test("a single host needs no comma"):
    assertEquals(AllowedHosts.parse("observatory.home.arpa"), Seq("observatory.home.arpa"))

  test("an empty value fails the boot instead of yielding an empty list"):
    // AllowedHostsFilter rejects every request when its list is empty, so `ALLOWED_HOSTS=` would otherwise produce a
    // service that starts, passes liveness, and answers 400 to everything.
    interceptMessage[IllegalArgumentException](
      s"requirement failed: ${AllowedHosts.ConfigKey} is empty; AllowedHostsFilter would reject every request"
    )(AllowedHosts.parse("  ,, "))
