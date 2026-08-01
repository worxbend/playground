/*
 * Copyright (c) 2020 Worxbend
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

package com.worxbend.cobalt

import com.typesafe.scalalogging.StrictLogging
import com.worxbend.persistence.repository.CheckpointStore
import java.time.Clock
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import org.apache.kafka.common.TopicPartition
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal

/** The consumer's lifecycle and observable state, behind one object.
  *
  * ## Why a supervisor at all
  *
  * Before this, the consumer was a stream started once at boot and drained once at shutdown, and the only way to stop
  * it was to stop the process. That is a poor answer to every real incident: a poison partition that needs skipping, a
  * database under maintenance, a backfill that must not be consumed while it runs, a group whose offsets are wrong.
  * All four end in `docker compose restart` plus a `kafka-consumer-groups.sh` invocation nobody has memorised, on a
  * container that has to be given a shell.
  *
  * ## The state machine, and what "pause" actually does
  *
  * **Pause tears the stream down; it does not idle it.** Pekko Connectors' `Consumer.Control` has no pause — it can
  * stop the fetcher, drain, or shut down, and nothing in between. Two implementations were available:
  *
  *   - hold the stream open and stop demand downstream, which stops *committing* but leaves the group's session alive
  *     and its partitions assigned. The broker keeps them assigned, so no other replica can take over, and
  *     `max.poll.interval.ms` eventually expires and forces the rebalance anyway — after a delay, unpredictably.
  *   - drain and stop, which commits everything in flight, leaves the group cleanly, and lets the partitions move to
  *     another replica immediately.
  *
  * The second is what an operator means by "pause this consumer", so that is what it does. `resume` materialises a
  * fresh stream, which rejoins the group and continues from the committed offsets. The cost is that a pause/resume
  * cycle is a rebalance; the benefit is that a paused replica is genuinely out of the way.
  *
  * ## Why one mutex and not an actor
  *
  * Every transition here is a slow, blocking, at-most-one-at-a-time operation: drain the stream, wait for the group to
  * leave, alter offsets, materialise a new stream. An actor would serialise them and add a mailbox, a protocol and a
  * set of ask timeouts to reason about. A single lock over an [[AtomicReference]] serialises them too, in eleven lines,
  * and the resulting code says plainly that a transition holds the consumer for its duration — which is the property
  * that matters and the one an ask-timeout would obscure.
  *
  * ## What this does not do
  *
  * It does not restart the stream on failure — [[ConsumerStream.restarting]] still owns that, with backoff. The
  * supervisor observes the outcome so it can report `failed` with the cause once the restart policy gives up. Two
  * restart mechanisms would fight.
  */
final class ConsumerSupervisor(
  factory: ConsumerFactory,
  offsets: AdminOffsets,
  checkpoints: CheckpointStore,
  val groupId: String,
  val topic: String,
  clock: Clock = Clock.systemUTC()
)(using ExecutionContext)
    extends StrictLogging:

  private val current = AtomicReference[ConsumerSupervisor.Snapshot](ConsumerSupervisor.Snapshot.initial(clock))

  /** The transition lock. Only lifecycle commands take it; [[status]] never does, so an operator can always ask what
    * is happening *while* a slow drain is happening — which is exactly when they want to.
    */
  private val transition = Object()

  /** The lifecycle state alone, without touching Kafka.
    *
    * Separate from [[status]] because the health probe calls it on a timer and an admin round trip per liveness check
    * would make the probe fail whenever the broker is slow — turning a broker hiccup into a container restart.
    */
  def state: RunState = current.get().state

  def running: Boolean = state.consuming

  /** The full picture: lifecycle state plus committed, stored and end offsets per partition.
    *
    * Every Kafka and database call here is best-effort. An admin timeout must not make this endpoint fail: the state
    * machine's own answer is always available and is the more important half, so a broker that cannot be reached
    * yields `null` positions and a state, not a 500. This is the endpoint somebody hits *because* something is wrong.
    */
  def status: Future[ConsumerStatus] =
    val snapshot = current.get()
    val stored = checkpoints
      .load(groupId)
      .map(rows => rows.map(row => TopicPartition(row.topic, row.partition) -> row.nextOffset).toMap)
      .recover { case NonFatal(error) => logger.warn(s"checkpoint read failed: ${error.getMessage}"); Map.empty }

    stored.map: storedOffsets =>
      val kafka = ConsumerSupervisor.quietly(logger, "kafka offsets")(readPositions()).getOrElse(Map.empty)
      val partitions = (kafka.keySet ++ storedOffsets.keySet).toList.sortBy(p => (p.topic, p.partition))
      val positions = partitions.map: partition =>
        val (committed, end) = kafka.getOrElse(partition, (None, None))
        PartitionPosition(
          topic = partition.topic,
          partition = partition.partition,
          committed = committed,
          stored = storedOffsets.get(partition),
          endOffset = end,
          // Lag needs both numbers. A partition with an end offset and no committed offset has *unknown* lag, not the
          // whole log's worth — reporting the latter is how an idle group looks like an emergency.
          lag = for
            c <- committed
            e <- end
          yield math.max(0L, e - c)
        )
      ConsumerStatus(
        state = snapshot.state,
        since = snapshot.since,
        generation = snapshot.generation,
        groupId = groupId,
        topic = topic,
        consuming = snapshot.state.consuming,
        lastError = snapshot.lastError,
        restarts = snapshot.restarts,
        positions = positions,
        totalLag = Option.when(positions.exists(_.lag.isDefined))(positions.flatMap(_.lag).sum)
      )

  /** Starts the stream if it is not already running. Idempotent. */
  def start(): Future[LifecycleResult] = command("start"):
    if current.get().state.consuming then false
    else
      materialise()
      true

  /** Drains and stops. Idempotent; a second call on a stopped consumer changes nothing and says so. */
  def stop(): Future[LifecycleResult] = command("stop"):
    if !current.get().state.consuming then false
    else
      drain(RunState.Stopped)
      true

  /** Drains and stops, marking the state `paused` rather than `stopped`.
    *
    * The distinction is *intent*, and it is worth a state of its own: an alert on "not consuming" should fire for
    * `stopped` and stay quiet for `paused`, because one of them is somebody deliberately holding the pipeline while
    * they work on the database.
    */
  def pause(): Future[LifecycleResult] = command("pause"):
    if current.get().state == RunState.Paused then false
    else
      if current.get().state.consuming then drain(RunState.Paused) else transitionTo(RunState.Paused)
      true

  def resume(): Future[LifecycleResult] = command("resume"):
    if current.get().state.consuming then false
    else
      materialise()
      true

  /** Stops, optionally moves the group's offsets, and starts again.
    *
    * **The stop is not optional and the order is not negotiable.** Kafka refuses `alterConsumerGroupOffsets` while the
    * group has live members, and it is right to: an offset moved under a running consumer is overwritten by that
    * consumer's next commit, so the call would appear to succeed and change nothing. Draining first is what makes the
    * seek take effect.
    *
    * Returns the offsets it actually set, so an operator sees what `earliest` or `stored` resolved to rather than
    * having to ask afterwards.
    */
  def restart(target: SeekTarget, explicit: Vector[SeekOffset]): Future[Either[String, RestartOutcome]] =
    Future:
      transition.synchronized:
        val from = current.get().state
        resolve(target, explicit) match
          case Left(problem) => Left(problem)
          case Right(seek)   =>
            if from.consuming then drain(RunState.Stopped)
            try
              offsets.alterOffsets(groupId, seek)
              materialise()
              logger.info(s"restarted at ${target.name}: ${seek.size} partition(s) moved")
              Right(RestartOutcome(target, seek.toList.map((p, o) => SeekOffset(p.topic, p.partition, o)).sortBy(s => (s.topic, s.partition)), from))
            catch
              case NonFatal(error) =>
                // The consumer is stopped and the seek failed: say so, and leave it stopped rather than starting it
                // against offsets nobody chose. An operator can retry; a silent start cannot be undone.
                fail(error)
                Left(s"could not move offsets: ${Option(error.getMessage).getOrElse(error.getClass.getName)}")

  /** Resolves a [[SeekTarget]] into concrete offsets, or explains why it cannot.
    *
    * `Committed` resolves to nothing at all, deliberately: "restart where you were" means *do not touch the offsets*,
    * and an alter call that rewrote them to the values just read would be a no-op with a race in it.
    */
  private def resolve(target: SeekTarget, explicit: Vector[SeekOffset]): Either[String, Map[TopicPartition, Long]] =
    try
      target match
        case SeekTarget.Committed => Right(Map.empty)
        case SeekTarget.Earliest  => Right(offsets.logStarts(offsets.partitionsOf(topic)))
        case SeekTarget.Latest    => Right(offsets.logEnds(offsets.partitionsOf(topic)))
        case SeekTarget.Explicit  =>
          if explicit.isEmpty then Left("target=explicit needs at least one 'topic/partition/offset'")
          else Right(explicit.map(spec => TopicPartition(spec.topic, spec.partition) -> spec.offset).toMap)
        case SeekTarget.Stored =>
          // Blocking on the admin path only. The alternative — threading a Future through a synchronized transition —
          // would make the lock's scope a promise chain, which is how a lock stops meaning what it says.
          val rows = scala.concurrent.Await.result(checkpoints.load(groupId), ConsumerSupervisor.StoreTimeout)
          if rows.isEmpty then Left(s"no stored checkpoints for group '$groupId'; nothing to seek to")
          else Right(rows.map(row => TopicPartition(row.topic, row.partition) -> row.nextOffset).toMap)
    catch case NonFatal(error) => Left(s"could not resolve $target: ${Option(error.getMessage).getOrElse("")}")

  /** Forgets the externalised checkpoints. Does not touch Kafka's own offsets — see the admin route's description. */
  def clearCheckpoints(): Future[Int] = checkpoints.clear(groupId)

  // --- internals ---------------------------------------------------------------------------------------------------

  /** Runs a transition under the lock and reports the state on both sides of it. */
  private def command(name: String)(act: => Boolean): Future[LifecycleResult] =
    val (from, changed) = transition.synchronized:
      val before = current.get().state
      (before, act)
    status.map(after => LifecycleResult(name, from, after, changed))

  private def materialise(): Unit =
    transitionTo(RunState.Starting)
    val handle = factory.start()
    current.updateAndGet(s => s.copy(handle = Some(handle), generation = s.generation + 1))
    // The stream is watched, not awaited. `restarting` retries on failure with backoff, so this callback fires only
    // once that policy is exhausted — which is exactly the moment `failed` becomes the honest state.
    handle.completion.onComplete:
      case Success(_) =>
        // Normal completion means a drain, and `drain` has already set the state it intended. Overwriting it here
        // would turn every deliberate pause into a `stopped` a moment later.
        ()
      case Failure(error) => fail(error)
    transitionTo(RunState.Running)

  private def drain(into: RunState): Unit =
    transitionTo(RunState.Stopping)
    current.get().handle.foreach: handle =>
      try scala.concurrent.Await.result(handle.drain(), ConsumerSupervisor.DrainTimeout)
      catch
        case NonFatal(error) =>
          // A drain that times out has still stopped the fetcher; the in-flight batch may not have committed, which
          // means a replay on the next start. Logged rather than surfaced as a failure because the consumer *is*
          // stopped, which is what was asked for.
          logger.warn(s"the consumer did not drain cleanly: ${Option(error.getMessage).getOrElse("")}")
    current.updateAndGet(_.copy(handle = None))
    transitionTo(into)

  private def transitionTo(state: RunState): Unit =
    val updated = current.updateAndGet(s => s.copy(state = state, since = clock.instant()))
    logger.info(s"consumer is ${updated.state.name} (generation ${updated.generation})")

  private def fail(error: Throwable): Unit =
    val message = Option(error.getMessage).getOrElse(error.getClass.getName)
    logger.error(s"the consumer stream failed: $message", error)
    val _ = current.updateAndGet(s =>
      s.copy(state = RunState.Failed, since = clock.instant(), lastError = Some(message), restarts = s.restarts + 1)
    )

  /** Committed and end offsets in one pass, as `(committed, end)` per partition. */
  private def readPositions(): Map[TopicPartition, (Option[Long], Option[Long])] =
    val committed = offsets.committed(groupId)
    val partitions = offsets.partitionsOf(topic) ++ committed.keySet
    val ends = offsets.logEnds(partitions)
    partitions.iterator.map(p => p -> (committed.get(p), ends.get(p))).toMap

/** What a restart did. */
final case class RestartOutcome(target: SeekTarget, offsets: List[SeekOffset], from: RunState)

/** A running stream, as the supervisor sees it. One method to stop it, one future to watch.
  *
  * An interface and not [[EventConsumer]] directly, so the supervisor's state machine can be tested against a handle
  * that needs no broker — which is the only way the pause/resume/failure transitions get covered at all.
  */
trait ConsumerHandle:
  def drain(): Future[org.apache.pekko.Done]
  def completion: Future[org.apache.pekko.Done]

/** Makes a fresh stream. Called once per `start` or `resume`; a stream is not restartable once drained. */
trait ConsumerFactory:
  def start(): ConsumerHandle

object ConsumerSupervisor:

  /** How long a drain may take before the supervisor gives up waiting and reports the consumer stopped anyway.
    *
    * Longer than `ConsumerConfig.drainTimeout`, so the connector's own bound is the one that normally fires and this
    * is only the backstop for a drain that hangs entirely.
    */
  val DrainTimeout: scala.concurrent.duration.FiniteDuration =
    scala.concurrent.duration.Duration(60, java.util.concurrent.TimeUnit.SECONDS)

  val StoreTimeout: scala.concurrent.duration.FiniteDuration =
    scala.concurrent.duration.Duration(10, java.util.concurrent.TimeUnit.SECONDS)

  /** The supervisor's whole mutable state, as one immutable value behind one reference.
    *
    * One `AtomicReference` to a case class rather than five references: a reader of [[ConsumerSupervisor.status]] gets
    * a *consistent* view, where five separate atomics would let it observe `state = running` alongside the previous
    * generation's error.
    */
  final case class Snapshot(
    state: RunState,
    since: Instant,
    generation: Int,
    restarts: Int,
    lastError: Option[String],
    handle: Option[ConsumerHandle]
  )

  object Snapshot:
    def initial(clock: Clock): Snapshot = Snapshot(RunState.Stopped, clock.instant(), 0, 0, None, None)

  /** Runs a best-effort call, logging rather than propagating.
    *
    * Used for every Kafka round trip on the status path. The state machine's answer is always available and is the
    * more important half; an admin timeout must degrade the response, not fail it.
    */
  private[cobalt] def quietly[A](logger: com.typesafe.scalalogging.Logger, what: String)(act: => A): Option[A] =
    try Some(act)
    catch
      case NonFatal(error) =>
        logger.warn(s"$what unavailable: ${Option(error.getMessage).getOrElse(error.getClass.getName)}")
        None
