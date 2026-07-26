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

package io.kzonix.observability

import java.net.InetAddress
import scala.util.Try

/** Identity of the running process, as every signal sees it.
  *
  * The three services must label metrics, resource attributes and log lines from the *same* three strings, because a
  * dashboard filters on `service`, a trace search filters on `service.name`, and a log query filters on `service` — if
  * those are derived independently, a rename lands in one place and silently orphans the other two. Constructing this
  * once and threading it through [[Telemetry]] is the only mechanism that keeps them equal.
  *
  * @param serviceName
  *   the deployment unit: `ferrite`, `cobalt`, `wolfram`. Low cardinality by construction.
  * @param serviceVersion
  *   the build version. Low cardinality *per deploy* but unbounded over time — which is exactly what makes it a useful
  *   metric tag (you can see the old and new versions overlap during a rollout) and a poor one to add more of.
  * @param instanceId
  *   the replica. See [[Telemetry]] for why this is deliberately kept out of the Micrometer common tags by default.
  */
final case class TelemetryConfig(serviceName: String, serviceVersion: String, instanceId: String):

  /** The OTel resource attribute string this config corresponds to.
    *
    * Kept next to the metric tags rather than in `Tracing` so the two representations of one identity are visibly
    * derived from the same fields.
    */
  private[observability] def resourceAttributes: String =
    s"service.version=$serviceVersion,service.instance.id=$instanceId"

object TelemetryConfig:

  /** Env var carrying the build version. Read here rather than from a generated `BuildInfo` object because all three
    * images are built from one pipeline that already exports it, and a generated constant would make the version a
    * property of the *jar* rather than of the *deploy*.
    */
  val VersionEnv: String = "SERVICE_VERSION"

  /** Env var carrying the replica identity. `HOSTNAME` is what both Docker and Kubernetes set, and under Kubernetes it
    * equals the pod name, so no downward-API wiring is needed.
    */
  val InstanceEnv: String = "HOSTNAME"

  /** Used when the version is genuinely unknown. Deliberately not `"latest"` or the empty string: an obviously-wrong
    * value in a dashboard legend is a bug report, whereas an empty tag looks like a rendering glitch.
    */
  val UnknownVersion: String = "0.0.0-unknown"

  /** Builds the config from the process environment.
    *
    * `serviceName` is a parameter and not an env var on purpose — it is a compile-time property of which `main` is
    * running, and making it configurable invites two replicas of the same service reporting under different names.
    *
    * @param env
    *   injected for testability; production callers use the default.
    */
  def fromEnv(serviceName: String, env: String => Option[String] = sys.env.get): TelemetryConfig =
    TelemetryConfig(
      serviceName = serviceName,
      serviceVersion = env(VersionEnv).filter(_.nonEmpty).getOrElse(UnknownVersion),
      instanceId = env(InstanceEnv).filter(_.nonEmpty).orElse(localHostName).getOrElse("unknown")
    )

  /** A reverse-DNS lookup can block for the DNS timeout on a misconfigured host, so the failure is swallowed rather
    * than allowed to abort process startup: telemetry identity is never worth failing a boot over.
    */
  private def localHostName: Option[String] =
    Try(InetAddress.getLocalHost.getHostName).toOption.filter(_.nonEmpty)
