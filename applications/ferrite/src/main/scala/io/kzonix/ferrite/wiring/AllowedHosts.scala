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

package io.kzonix.ferrite.wiring

import jakarta.inject.Inject
import jakarta.inject.Provider
import jakarta.inject.Singleton
import play.api.Configuration
import play.filters.hosts.AllowedHostsConfig

/** Parses the operator-facing `ALLOWED_HOSTS` setting into the list `AllowedHostsFilter` needs.
  *
  * **Why this exists at all.** Play declares `play.filters.hosts.allowed` as a *list*, and a HOCON environment
  * substitution can only ever produce a string. `play.filters.hosts.allowed = ${?ALLOWED_HOSTS}` therefore looks
  * correct, validates with `docker compose config`, and then kills the boot with
  * `play.filters.hosts.allowed has type STRING rather than LIST` the first time the variable is actually set — which is
  * every deployment and no development run, the worst possible place for a failure to hide.
  *
  * The alternative Play offers is indexed system properties (`-Dplay.filters.hosts.allowed.0=…
  * -Dplay.filters.hosts.allowed.1=…`), which Lightbend Config does reassemble into a list. That is rejected: it makes
  * the operator hand-maintain array indices in a `JAVA_OPTS` string, and a skipped index silently truncates the list,
  * which fails as a 403 on one hostname rather than as an error.
  *
  * So the string stays a string, under a key of ferrite's own, and this is the one place that splits it.
  */
object AllowedHosts:

  /** The configuration key holding the raw comma-separated form. Not `play.filters.hosts.allowed`: that key is typed as
    * a list by Play's own `reference.conf`, and giving it a string here would break `Configuration`'s type checking for
    * anything else that reads it.
    */
  val ConfigKey: String = "ferrite.allowed-hosts"

  /** Splits on commas, trims, and drops empties, so `"localhost, 127.0.0.1,"` means what it looks like it means.
    *
    * An empty result is a boot failure rather than an empty list. `AllowedHostsFilter` rejects *every* request with 403
    * when its list is empty, so `ALLOWED_HOSTS=` — a typo, or an unset variable that a shell expanded to nothing —
    * would otherwise produce a service that starts, passes its liveness probe, and answers nothing.
    */
  def parse(raw: String): Seq[String] =
    val hosts = raw.split(',').iterator.map(_.trim).filter(_.nonEmpty).toSeq
    require(hosts.nonEmpty, s"$ConfigKey is empty; AllowedHostsFilter would reject every request")
    hosts

/** Supplies [[AllowedHostsConfig]] in place of Play's own provider.
  *
  * `play.modules.disabled += "play.filters.hosts.AllowedHostsModule"` in `application.conf` is what makes this the
  * binding rather than a duplicate of one — Play's module binds the same type, and Guice treats two bindings for one
  * type as an error, not as an override.
  *
  * `shouldProtect` is left at its default (protect everything). Play's provider derives it from the
  * `routeModifiers.whiteList`/`blackList` settings, which match modifiers written into a compiled `conf/routes` file;
  * ferrite routes with SIRD and has no routes file (ADR §8.1), so those settings could never match anything here and
  * reading them would only suggest they work.
  */
@Singleton
final class AllowedHostsProvider @Inject() (configuration: Configuration) extends Provider[AllowedHostsConfig]:

  def get(): AllowedHostsConfig =
    AllowedHostsConfig(AllowedHosts.parse(configuration.get[String](AllowedHosts.ConfigKey)))
