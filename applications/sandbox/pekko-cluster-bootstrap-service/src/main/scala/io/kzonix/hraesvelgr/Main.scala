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

package io.kzonix.hraesvelgr

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.ClusterEvent.MemberEvent
import org.apache.pekko.cluster.typed.Cluster
import org.apache.pekko.cluster.typed.Subscribe

/** Minimal Pekko cluster node.
  *
  * Replaces the empty `class Main {}` this module used to contain: it now starts an actor system, joins the cluster and
  * reports membership transitions, so the module does what its name says.
  */
object Main:

  private val Name = "hraesvelgr"

  private def rootBehavior: Behavior[MemberEvent] =
    Behaviors.setup: context =>
      val cluster = Cluster(context.system)
      context.log.info(s"Cluster node starting at ${cluster.selfMember.address}")
      cluster.subscriptions ! Subscribe(context.self, classOf[MemberEvent])

      Behaviors.receiveMessage: event =>
        context.log.info(s"Cluster membership changed: $event")
        Behaviors.same

  def main(args: Array[String]): Unit =
    val config = ConfigFactory.load()
    val _      = ActorSystem(rootBehavior, Name, config)
