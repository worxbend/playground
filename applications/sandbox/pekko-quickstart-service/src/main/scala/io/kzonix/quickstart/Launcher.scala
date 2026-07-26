package io.kzonix.quickstart

import akka.actor.typed.Behavior
import akka.actor.typed.internal.BehaviorImpl
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.Config

import scala.concurrent.Future

object Launcher {

  sealed trait ApplicationLifecycle
  case class ApplicationStart(config: Config) extends ApplicationLifecycle
  case class ApplicationStop(msg: String)     extends ApplicationLifecycle

  def apply(): Behavior[ApplicationLifecycle] =
    Behaviors.setup { context =>
      context.log.info("Initializing application guardian actor behaviour...")
    launcherBehaviour(false)
    }

  private def launcherBehaviour(isStarted: Boolean): Behavior[ApplicationLifecycle] =
    Behaviors.receive[ApplicationLifecycle] { (context, msg) =>
      msg match {
        case ApplicationStart(config) =>
          if (isStarted) {
            context.log.info(s"Application has been already started... Ignoring message.")
            launcherBehaviour(isStarted = true)
          } else {
            context.log.info("Starting...")
            start(
              config,
              context
            )
            context.log.info("Started.")
            launcherBehaviour(isStarted = true)
          }

        case ApplicationStop(info) =>
          context.log.info(s"Stop with message '$info''")
          Behaviors.stopped(
            postStop = () => context.log.info(s"${ context.self.toString } stopped.")
          )
      }
    }

  def start(config: Config, context: ActorContext[ApplicationLifecycle]): Unit =
    // TODO: parsing configuration
    //  -- start DB connection pool
    //  -- start HTTP-client dispatcher/connection pool
    //  -- start metrics registry
    //  -- start consumer
    //  -- start scheduled actors (background tasks)
    //  --- start producer
    ()

}
