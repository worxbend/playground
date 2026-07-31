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

package com.worxbend.ferrite.wiring

import com.worxbend.observability.Telemetry
import com.worxbend.persistence.repository.EventRepository
import java.time.Clock
import play.api.Configuration
import play.api.Environment
import play.api.inject.Binding
import play.api.inject.Module
import play.filters.hosts.AllowedHostsConfig

/** Every binding this application adds to Play's defaults, in one file.
  *
  * A `play.api.inject.Module` rather than a Guice `AbstractModule`: its `bindings` method returns a *value*, so the
  * whole wiring is a list that can be read at a glance and — because it is data — could be asserted on. A Guice module
  * expresses the same thing as a sequence of side effects, each of which also returns a builder that `-Werror` with
  * `-Wnonunit-statement` would make you bind to `val _ =`.
  *
  * Constructor injection everywhere and no global state, so every one of these types can be constructed by hand in a
  * test. The two that could not be — [[Clock]] and [[Telemetry]] — are the reason this file exists: `Clock` is abstract
  * and `Telemetry` has a private constructor and a shutdown obligation.
  */
final class FerriteModule extends Module:

  override def bindings(environment: Environment, configuration: Configuration): Seq[Binding[?]] =
    Seq(
      // UTC, not the system default. Every timestamp in this application is a `timestamptz` rendered as RFC 3339, and
      // a server whose zone is not UTC would render the *same* instant differently from its neighbour.
      bind[Clock].toInstance(Clock.systemUTC()),
      // Eager: the pools must fail the boot, not the first request. See `Databases`.
      bind[Databases].toSelf.eagerly(),
      bind[EventRepository].toProvider[EventRepositoryProvider],
      bind[Readiness].to[DatabaseReadiness],
      bind[Telemetry].toProvider[TelemetryProvider],
      // Replaces Play's own provider, which is disabled in `application.conf`. See `AllowedHosts` for why a
      // comma-separated environment variable cannot reach `play.filters.hosts.allowed` directly.
      bind[AllowedHostsConfig].toProvider[AllowedHostsProvider]
    )
