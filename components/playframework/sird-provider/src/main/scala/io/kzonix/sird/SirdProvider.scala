package io.kzonix.sird

import jakarta.inject.Inject
import jakarta.inject.Provider
import jakarta.inject.Singleton
import play.api.http.HttpConfiguration
import play.api.routing.Router

/** Composes every [[ProvidedRouter]] bound in the injector into the single [[Router]] Play serves.
  *
  * Each router is mounted under the application context path joined with its own prefix, then folded together with
  * [[Router.orElse]]. The fold starts from [[Router.empty]] on purpose: a service that registers no routers serves 404s
  * instead of failing injector creation, which keeps a misconfigured module from taking the process down at boot.
  */
@Singleton
final class SirdProvider @Inject() (
    routers: Set[ProvidedRouter],
    httpConfig: HttpConfiguration
) extends Provider[Router]:

  override def get(): Router =
    routers
      .map(router => router.withPrefix(Router.concatPrefix(httpConfig.context, router.prefix)))
      .foldLeft(Router.empty)((composed, next) => composed.orElse(next))
