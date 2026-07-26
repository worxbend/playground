package io.kzonix.quickstart

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import com.typesafe.config.ConfigFactory
import io.kzonix.quickstart.Launcher.ApplicationStart
import io.kzonix.quickstart.Launcher.ApplicationStop
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future

object Main {
  private val Name = "quickstart"

  def main(args: Array[String]): Unit = {

    // Happens before creating the actor system to fail fast
    val config = ConfigFactory.load("application.conf")

    // Use a classic system, because some libraries still rely on it
    implicit val as                           = ActorSystem(
      Launcher(),
      Name,
      config
    )
    implicit val ec: ExecutionContextExecutor = as.executionContext

    val cs = CoordinatedShutdown(as)

    as ! ApplicationStart(config)

    cs.addTask(
      CoordinatedShutdown.PhaseBeforeServiceUnbind,
      "application-stop"
    ) { () =>
       as ! ApplicationStop("Oops...")
       Future.successful(Done)
    }

    cs.addTask(
      CoordinatedShutdown.PhaseBeforeActorSystemTerminate,
      "application-termination"
    ) { () =>
      Future.successful(Done)
    }
  }

}
