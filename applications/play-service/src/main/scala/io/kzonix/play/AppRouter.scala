package io.kzonix.play

import jakarta.inject.Inject
import play.api.routing.Router.Routes
import play.api.routing.SimpleRouter
import play.api.routing.sird.GET
import play.api.routing.sird.UrlContext

/** The application's routing table.
  *
  * Selected via `play.http.router` in `application.conf`, using Play's SIRD string interpolation. There is no
  * `conf/routes` file, which is what lets this service run on `PlayService` without the routes compiler.
  */
final class AppRouter @Inject() (controller: AppController) extends SimpleRouter:

  override def routes: Routes =
    case GET(p"/health")      => controller.health
    case GET(p"/greet/$name") => controller.greet(name)
