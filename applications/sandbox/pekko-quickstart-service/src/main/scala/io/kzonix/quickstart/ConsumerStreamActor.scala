package io.kzonix.quickstart

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerSettings
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Committer
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.stream.scaladsl.RunnableGraph
import io.kzonix.quickstart.ConsumerStreamActor.ConsumerCommand
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class ConsumerStreamActor(context: ActorContext[ConsumerCommand])(implicit ac: ClassicActorSystemProvider) {

  val controlContainer = new AtomicReference[Consumer.Control](Consumer.NoopControl)

  def start(): Unit = {
    val consumerSettings: ConsumerSettings[String, String] = getConsumerSettings(ac.classicSystem)

    val control: RunnableGraph[DrainingControl[Done]] = Consumer
      .committableSource(
        consumerSettings,
        Subscriptions.topics("backblaze_smart")
      )
      .groupedWithin(
        100,
        100.milliseconds
      )
      .mapAsync(10) { msg =>
        Future {
          context.log.info(s"Processing $msg")
          CommittableOffsetBatch(msg.map(_.committableOffset))
        }(context.executionContext)
      }
      .toMat(Committer.sink(CommitterSettings(context.system)))(DrainingControl.apply)

    val drainingControl: DrainingControl[Done] = control.run()
    controlContainer.set(drainingControl)
  }

  def stop(): Unit =
    Option(controlContainer.getOpaque) match {
      case Some(noOp @ Consumer.NoopControl)            => context.log.info("Stream is not started yet")
      case Some(drainingControl: DrainingControl[Done]) => drainingControl.drainAndShutdown()(context.executionContext)
    }

  private def getConsumerSettings(as: ClassicActorSystemProvider): ConsumerSettings[String, String] =
    ConsumerSettings(
      as,
      new StringDeserializer,
      new StringDeserializer
    )
      .withGroupId("group2")
      .withProperty(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        "earliest"
      )

}

object ConsumerStreamActor {

  sealed trait ConsumerCommand
  case class StartConsumer(topic: String) extends ConsumerCommand
  case class StopConsumer(topic: String)  extends ConsumerCommand

  def apply(): Behavior[ConsumerCommand] =
    Behaviors.setup { context =>
      Behaviors.receive[ConsumerCommand] { (_, msg) =>

         implicit val classicActorSystemProvider: ActorSystem[Nothing] = context.system
         msg match {
           case StartConsumer(topic) =>
             new ConsumerStreamActor(context).start()
             Behaviors.same

           case StopConsumer(topic) => Behaviors.same
         }
      }

    }

}
