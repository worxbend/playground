/*
 * Copyright (c) 2020 Kzonix Projects
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package io.kzonix.quickstart

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.PostStop
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.kafka.CommitterSettings
import org.apache.pekko.kafka.ConsumerMessage.CommittableOffsetBatch
import org.apache.pekko.kafka.ConsumerSettings
import org.apache.pekko.kafka.Subscriptions
import org.apache.pekko.kafka.scaladsl.Committer
import org.apache.pekko.kafka.scaladsl.Consumer
import org.apache.pekko.kafka.scaladsl.Consumer.DrainingControl
import org.apache.pekko.stream.Materializer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

/** Typed actor owning a committable Kafka consumer stream.
  *
  * The running stream's [[DrainingControl]] is carried in the behaviour rather than in an `AtomicReference`. The actor
  * is the only writer, so behaviour state makes "running or idle" total, replacing a partial match on a mutable cell
  * that had no case for an already-drained stream.
  */
object ConsumerStreamActor:

  sealed trait ConsumerCommand
  final case class StartConsumer(topic: String) extends ConsumerCommand
  final case class StopConsumer(topic: String)  extends ConsumerCommand

  def apply(groupId: String): Behavior[ConsumerCommand] =
    Behaviors.setup(context => idle(context, groupId))

  private def idle(context: ActorContext[ConsumerCommand], groupId: String): Behavior[ConsumerCommand] =
    Behaviors.receiveMessage:
      case StartConsumer(topic) =>
        context.log.info(s"Starting consumer for topic '$topic' in group '$groupId'")
        running(context, groupId, startStream(context, groupId, topic))
      case StopConsumer(topic) =>
        context.log.info(s"Consumer for topic '$topic' is not running")
        Behaviors.same

  private def running(
      context: ActorContext[ConsumerCommand],
      groupId: String,
      control: DrainingControl[Done]
  ): Behavior[ConsumerCommand] =
    Behaviors
      .receiveMessage[ConsumerCommand]:
        case StartConsumer(topic) =>
          context.log.info(s"Consumer for topic '$topic' is already running")
          Behaviors.same
        case StopConsumer(topic) =>
          context.log.info(s"Draining consumer for topic '$topic'")
          drain(context, control)
          idle(context, groupId)
      .receiveSignal:
        // Without this the stream outlives the actor and keeps consuming.
        case (_, PostStop) =>
          drain(context, control)
          Behaviors.same

  private def drain(context: ActorContext[ConsumerCommand], control: DrainingControl[Done]): Unit =
    given ExecutionContext = context.executionContext
    control
      .drainAndShutdown()
      .foreach(_ => context.log.info("Consumer stream drained"))

  private def startStream(
      context: ActorContext[ConsumerCommand],
      groupId: String,
      topic: String
  ): DrainingControl[Done] =
    Consumer
      .committableSource(consumerSettings(context, groupId), Subscriptions.topics(topic))
      .groupedWithin(100, 100.milliseconds)
      .map(batch => CommittableOffsetBatch(batch.map(_.committableOffset)))
      .toMat(Committer.sink(CommitterSettings(context.system)))(DrainingControl.apply)
      .run()(using Materializer(context.system))

  private def consumerSettings(
      context: ActorContext[ConsumerCommand],
      groupId: String
  ): ConsumerSettings[String, String] =
    ConsumerSettings(context.system, new StringDeserializer, new StringDeserializer)
      .withGroupId(groupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
