package io.kzonix.sird

import com.google.inject.AbstractModule
import net.codingwell.scalaguice.ScalaModule
import net.codingwell.scalaguice.ScalaMultibinder

/** Creates the `Set[ProvidedRouter]` multibinder unconditionally.
  *
  * Without this, a service that pulls in the routing component but registers no routers of its own has no
  * `Set[ProvidedRouter]` binding at all, and Guice fails injector creation at boot with an unhelpful "No implementation
  * for java.util.Set<ProvidedRouter> was bound". Creating the (possibly empty) set here means such a service starts and
  * serves 404s, which is diagnosable.
  */
final class SirdModule extends AbstractModule with ScalaModule:

  override def configure(): Unit =
    val _ = ScalaMultibinder.newSetBinder[ProvidedRouter](binder)
