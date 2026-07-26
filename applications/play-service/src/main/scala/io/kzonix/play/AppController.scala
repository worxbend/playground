package io.kzonix.play

import jakarta.inject.Inject
import jakarta.inject.Singleton
import play.api.libs.json.Json
import play.api.mvc.AbstractController
import play.api.mvc.Action
import play.api.mvc.AnyContent
import play.api.mvc.ControllerComponents

@Singleton
final class AppController @Inject() (cc: ControllerComponents) extends AbstractController(cc):

  /** Liveness probe. Consults no dependencies: a probe that fails during a downstream outage gets the container
    * restarted, turning a recoverable outage into a restart loop.
    */
  def health: Action[AnyContent] = Action:
    Ok(Json.obj("status" -> "UP"))

  def greet(name: String): Action[AnyContent] = Action:
    Ok(Json.obj("message" -> s"Hello, $name"))
