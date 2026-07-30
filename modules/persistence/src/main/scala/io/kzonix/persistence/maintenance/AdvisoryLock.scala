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

package io.kzonix.persistence.maintenance

import com.typesafe.scalalogging.StrictLogging
import java.sql.Connection
import javax.sql.DataSource
import scala.util.Using
import scala.util.control.NonFatal

/** The identity of a maintenance job, as PostgreSQL's two-integer advisory-lock key.
  *
  * **Two `int4`s and not one `bigint`, on purpose.** The two forms occupy the same lock space, but `pg_locks` renders
  * them differently: the two-integer form fills `classid` and `objid` with the numbers that were passed, while the
  * single-`bigint` form splits the value across those columns and leaves an operator reverse-engineering which job is
  * stuck. A hash of a job name would have the same problem *and* introduce collisions between jobs nobody would think
  * to look for. Small explicit ordinals under one namespace are debuggable and collision-free by construction.
  *
  * @param namespace
  *   distinguishes this application's locks from anything else sharing the database. Advisory locks are a single
  *   cluster-wide space with no ownership: a second application picking the same two numbers would silently serialise
  *   against these jobs.
  * @param job
  *   the ordinal within that namespace. Never reuse one for a different job — the reused number would make the new job
  *   and the old one mutually exclusive across a rolling deploy for no visible reason.
  */
final case class LockKey(namespace: Int, job: Int)

/** A non-blocking, session-scoped exclusive lease over one database.
  *
  * **Try-and-skip, never wait.** `pg_try_advisory_lock` returns immediately with a boolean; the blocking
  * `pg_advisory_lock` would queue. With N replicas and a fixed schedule, queueing is the wrong shape: the replica that
  * loses the race would sit on a pooled connection until the winner finished, and if the job ever takes longer than the
  * interval, the losers accumulate one waiter per tick until the pool is exhausted and the *ingest* path — which shares
  * that pool — starts timing out in `getConnection`. Skipping costs nothing: the work was done by somebody, and the
  * next tick is a few minutes away.
  *
  * **Session-scoped and not transaction-scoped.** `pg_try_advisory_xact_lock` releases at `COMMIT`, which would be
  * ideal, but the partition job deliberately runs its statements in autocommit — each `CREATE TABLE` is its own
  * transaction so a failure on month four does not roll back months one to three. A session lock spans them. The price
  * is that it must be released by hand, which is what [[AdvisoryLock.guarded]] exists to make unmissable: a lock left
  * behind on a *pooled* connection is not cleaned up until that physical connection is retired (`max-lifetime`, 25
  * minutes here), during which every replica skips every cycle and nothing looks broken.
  */
final class AdvisoryLock(dataSource: DataSource, val key: LockKey) extends StrictLogging:

  /** Runs `body` under the lock on a dedicated connection, or returns `None` if another session holds it.
    *
    * The connection is handed to `body` rather than opened again inside it, and that is a correctness requirement, not
    * a convenience: an advisory lock belongs to the *session* that took it, so work done on a second pooled connection
    * would be entirely unprotected while appearing to be guarded.
    */
  def tryRun[A](body: Connection => A): Option[A] =
    Using.resource(dataSource.getConnection): connection =>
      AdvisoryLock.guarded(() => acquire(connection), () => release(connection))(body(connection))

  private def acquire(connection: Connection): Boolean =
    Using.resource(connection.prepareStatement("SELECT pg_try_advisory_lock(?, ?)")): statement =>
      statement.setInt(1, key.namespace)
      statement.setInt(2, key.job)
      Using.resource(statement.executeQuery())(results => results.next() && results.getBoolean(1))

  /** Best-effort, and logged rather than raised.
    *
    * A throwing release inside a `finally` would replace whatever exception the job was already failing with — the
    * useful one — with a connection error. And the fallback is sound: the server drops every session-level advisory
    * lock when the session ends, so the worst case of a failed release is that the lock survives until the pool retires
    * that connection.
    */
  private def release(connection: Connection): Unit =
    try
      val released =
        Using.resource(connection.prepareStatement("SELECT pg_advisory_unlock(?, ?)")): statement =>
          statement.setInt(1, key.namespace)
          statement.setInt(2, key.job)
          Using.resource(statement.executeQuery())(results => results.next() && results.getBoolean(1))
      if !released then logger.warn(s"advisory lock $key was not held at release; another session may have taken it")
    catch
      case NonFatal(error) =>
        logger.warn(s"releasing advisory lock $key failed; it expires with the session", error)

object AdvisoryLock:

  /** This application's slice of the cluster-wide advisory-lock space. An arbitrary constant, fixed forever: changing
    * it during a rolling deploy would let the old and the new replicas run the same job at the same time, which is the
    * one thing these locks exist to prevent.
    */
  val Namespace: Int = 1799545188

  /** Partition creation and retention. */
  val Partitions: LockKey = LockKey(Namespace, 1)

  /** The hourly rollup refresh.
    *
    * A **separate** key from [[Partitions]], not a shared "maintenance" lock. The refresh is a full recompute that can
    * run for minutes on a large table; sharing one key would let it lock out partition creation, and a missing
    * partition is a hard ingest failure while a stale rollup is a slightly old dashboard. The two jobs touch different
    * objects and must be able to run at the same time.
    */
  val Rollup: LockKey = LockKey(Namespace, 2)

  /** The contention rule, isolated from anything that needs a database so it can be tested for the case that matters:
    * that `body` does not run when the lease was refused, and that the lease is released when `body` throws.
    *
    * `release` is called if and only if `acquire` returned `true`. Releasing a lock never taken is not harmless —
    * PostgreSQL logs a warning and, worse, an unbalanced release in a *reentrant* nesting would drop a lock the outer
    * caller still believes it holds.
    */
  def guarded[A](acquire: () => Boolean, release: () => Unit)(body: => A): Option[A] =
    if !acquire() then None
    else
      try Some(body)
      finally release()
