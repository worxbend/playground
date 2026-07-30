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

/** What one refresh of the hourly rollup did.
  *
  * @param rows
  *   rows in the materialized view afterwards. The number a dashboard is reading from, and the cheap way to notice that
  *   a refresh "succeeded" against a fact table somebody truncated.
  * @param concurrent
  *   whether `CONCURRENTLY` was used. `false` means the view was unpopulated and had to be built blocking — see
  *   [[RollupRefresh]] for when that can happen and why it is once.
  */
final case class RefreshReport(rows: Long, concurrent: Boolean)

/** Keeps `events.event_rollup_hourly` from being a snapshot of the moment the schema was created.
  *
  * **What goes wrong without this.** `V1__events.sql` creates the view, indexes it, and refreshes it once — against an
  * empty table, because the migration is the thing that just created that table. PostgreSQL materialized views are not
  * self-maintaining: nothing in the database updates one, ever. So every facet count, every dashboard aggregate and the
  * landing-page histogram read a permanently empty relation, and they do it *successfully* — no error, no empty result
  * set that looks wrong, just numbers that are stale by however long the deployment has been running. ADR §0 decision 7
  * chose a materialized view over consumer-maintained counters precisely because the view is idempotent and
  * authoritative; both of those are properties of a view that is refreshed.
  *
  * **Why `CONCURRENTLY`, and what it costs.** A plain `REFRESH MATERIALIZED VIEW` takes `ACCESS EXCLUSIVE` on the view:
  * every dashboard query blocks for the whole rebuild, which on this view is a 90-day aggregate over the fact table.
  * `CONCURRENTLY` instead builds the new contents into a temporary relation and then merges the difference, holding
  * only `EXCLUSIVE` — readers keep reading the old contents throughout and see the new ones atomically at the end. That
  * is only legal because `V1__events.sql` creates `event_rollup_hourly_uk`; a unique index over every grouping column
  * is the precondition, and the day that index is dropped this statement stops being a no-op for readers and silently
  * becomes a global stall.
  *
  * The cost is real and worth stating plainly: **`CONCURRENTLY` is not incremental.** It recomputes the entire view
  * from the fact table every time, then diffs. It is *more* expensive than a blocking refresh, not less — it trades
  * total work for the absence of a read lock. ADR §12.4's tripwire follows directly: when a refresh exceeds the
  * interval between refreshes, this design is finished and the replacement is a per-hour merge table maintained at
  * ingest (`INSERT ... ON CONFLICT DO UPDATE`), not a bigger machine. Metering the duration is how that moment is
  * noticed rather than inferred from a slow dashboard.
  */
final class RollupRefresh(dataSource: DataSource) extends StrictLogging:

  /** The lease. Distinct from the partition job's — see [[AdvisoryLock.Rollup]] for why they must not share one. */
  val lock: AdvisoryLock = AdvisoryLock(dataSource, AdvisoryLock.Rollup)

  /** One refresh, under the advisory lock. `None` means another replica is already refreshing.
    *
    * Skipping is exactly right here and not merely acceptable: two concurrent refreshes would each do the full
    * recompute and the second would produce the same answer the first is about to install. The refresh is idempotent,
    * so the loser has nothing to catch up on.
    */
  def run(): Option[RefreshReport] = lock.tryRun(runOn)

  /** The refresh itself, without the lock. Package-visible so an integration test can prove that a contended run
    * declines while an uncontended one does the work.
    */
  private[persistence] def runOn(connection: Connection): RefreshReport =
    // `CONCURRENTLY` is rejected outright on a view that has never been populated ("CONCURRENTLY cannot be used when
    // the materialized view is not populated"). V1 populates it, so this branch is not for the normal path — it is
    // for a database restored from a dump taken with `--no-data`, or one where an operator ran `REFRESH ... WITH NO
    // DATA` to reclaim space. Falling back once, rather than failing every five minutes forever, is the difference
    // between a self-healing schedule and a permanently red job nobody can act on.
    val populated = isPopulated(connection)
    if !populated then
      logger.warn(
        s"${RollupRefresh.View} is unpopulated, so this refresh is blocking and will hold ACCESS EXCLUSIVE on it; " +
          "subsequent refreshes go back to CONCURRENTLY"
      )
    execute(connection, if populated then RollupRefresh.RefreshConcurrently else RollupRefresh.RefreshBlocking)
    val rows = count(connection)
    logger.debug(s"refreshed ${RollupRefresh.View}: $rows row(s)")
    RefreshReport(rows, populated)

  private def isPopulated(connection: Connection): Boolean =
    Using.resource(connection.createStatement()): statement =>
      Using.resource(statement.executeQuery(RollupRefresh.IsPopulated)): results =>
        results.next() && results.getBoolean(1)

  private def count(connection: Connection): Long =
    Using.resource(connection.createStatement()): statement =>
      Using.resource(statement.executeQuery(RollupRefresh.CountRows)): results =>
        if results.next() then results.getLong(1) else 0L

  private def execute(connection: Connection, sql: String): Unit =
    Using.resource(connection.createStatement()): statement =>
      val _ = statement.execute(sql)

object RollupRefresh:

  /** The view `V1__events.sql` creates. */
  val View: String = "events.event_rollup_hourly"

  /** Legal only because of `event_rollup_hourly_uk`. See the class Scaladoc. */
  val RefreshConcurrently: String = s"REFRESH MATERIALIZED VIEW CONCURRENTLY $View"

  /** The fallback for a view that has never held data, which `CONCURRENTLY` refuses to touch. */
  val RefreshBlocking: String = s"REFRESH MATERIALIZED VIEW $View"

  val IsPopulated: String =
    "SELECT c.relispopulated FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
      "WHERE n.nspname = 'events' AND c.relname = 'event_rollup_hourly'"

  val CountRows: String = s"SELECT count(*) FROM $View"
