package io.kzonix.play

import io.kzonix.sird.SirdProvider
import play.api.ApplicationLoader
import play.api.inject.bind
import play.api.inject.guice.GuiceApplicationLoader
import play.api.inject.guice.GuiceableModule
import play.api.routing.Router

/** Bootstraps the application with Guice and routes it through [[SirdProvider]] instead of a compiled `conf/routes`
  * file.
  *
  * Enabled by this module's `reference.conf`, so a service gets SIRD routing simply by depending on `play-utile`.
  */
final class SimpleApplicationLoader extends GuiceApplicationLoader:

  override protected def overrides(context: ApplicationLoader.Context): Seq[GuiceableModule] =
    super.overrides(context) :+ (bind[Router].toProvider[SirdProvider]: GuiceableModule)
