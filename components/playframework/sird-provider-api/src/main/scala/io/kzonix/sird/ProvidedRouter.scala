package io.kzonix.sird

import play.api.routing.Router

/** A router contributed to the application-wide routing table.
  *
  * Implementations declare where they mount via [[routePrefix]] and define their routes with Play's SIRD string
  * interpolation. [[SirdProvider]] collects every binding and composes them into the single [[Router]] Play serves.
  */
trait ProvidedRouter extends Router:

  val routePrefix: RoutePrefix

  final lazy val prefix: String = ProvidedRouter.routeWithVersion(routePrefix)

private[sird] object ProvidedRouter:

  /** Prepends the `vN` segment when the prefix is versioned, leaving the path untouched otherwise. */
  private[sird] def routeWithVersion(routePrefix: RoutePrefix): String =
    if routePrefix.isVersional then Router.concatPrefix(s"v${routePrefix.version}", routePrefix.prefix)
    else routePrefix.prefix
