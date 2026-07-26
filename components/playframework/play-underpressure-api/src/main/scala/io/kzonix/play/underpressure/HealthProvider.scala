package io.kzonix.play.underpressure

import scala.concurrent.Future

/** Outcome of a single dependency check. */
enum HealthStatus:
  case Up, Degraded, Down

final case class HealthCheck(
    name: String,
    status: HealthStatus,
    detail: Option[String] = None
)

object HealthCheck:
  def up(name: String): HealthCheck                    = HealthCheck(name, HealthStatus.Up)
  def down(name: String, reason: String): HealthCheck  = HealthCheck(name, HealthStatus.Down, Some(reason))
  def degraded(name: String, why: String): HealthCheck = HealthCheck(name, HealthStatus.Degraded, Some(why))

/** A dependency that contributes to a service's readiness.
  *
  * Bind implementations into the `Set[HealthProvider]` multibinder; `HealthModule` exposes the aggregate at
  * `/health/ready`. Implementations must not throw — return a `Down` check instead, and apply their own timeout.
  */
trait HealthProvider:

  def name: String

  def check(): Future[HealthCheck]
