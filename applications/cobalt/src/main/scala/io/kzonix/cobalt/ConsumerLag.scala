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

package io.kzonix.cobalt

import io.kzonix.observability.Meters
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.MultiGauge
import io.micrometer.core.instrument.Tags
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.common.TopicPartition
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

/** The lag arithmetic, separated from every I/O that feeds it.
  *
  * Lag is the single most important number this service publishes and it is trivially easy to get subtly wrong — off by
  * one, negative during a reset, or fabricated for a partition nobody has ever committed to. Keeping it as a pure
  * function over two maps is what makes those cases testable without a broker.
  */
object ConsumerLag:

  /** Lag per partition: how many records sit between the group's committed offset and the log end.
    *
    * Two rules, both of which exist because the alternative produces a false alarm:
    *
    *   - **A partition with no committed offset is omitted, not reported as zero and not reported as `end`.** A group
    *     that has never committed genuinely has unknown lag: reporting `end` pages the on-call the first time a topic
    *     is created, reporting `0` claims a consumer is caught up when it has not read a single record. Omitting it
    *     leaves a gap in the series, which is the honest rendering of "no data".
    *   - **Negative differences clamp to zero.** The committed offset can legitimately exceed the log end for a moment
    *     after a topic truncation, a partition reassignment, or an offset reset, and a negative lag on a dashboard is
    *     read as a broken exporter rather than as the transient it is.
    */
  def lags(committed: Map[TopicPartition, Long], logEnds: Map[TopicPartition, Long]): Map[TopicPartition, Long] =
    committed.flatMap: (partition, offset) =>
      logEnds.get(partition).map(end => partition -> math.max(0L, end - offset))

  /** Total lag across the group. The number an alert threshold is actually written against — per-partition lag answers
    * "which one", but only the sum answers "is the consumer falling behind".
    */
  def total(lags: Map[TopicPartition, Long]): Long = lags.valuesIterator.sum

/** Publishes [[ConsumerLag]] as a Micrometer `MultiGauge`, fed from a single reused `AdminClient`.
  *
  * **An `AdminClient` and not the consumer's own `records-lag-max`** (ADR §7.1). The client metric only covers
  * partitions the consumer is currently fetching: it reads zero during a rebalance and disappears entirely when the
  * process is down — the two moments at which lag is the only number anyone wants. Asking the broker instead means the
  * measurement survives the thing it is measuring.
  *
  * **One `AdminClient`, reused.** Each one opens its own connections and its own metrics; constructing one per poll
  * turns a monitoring signal into a connection-churn problem on the broker.
  *
  * `MultiGauge.register(rows, overwrite = true)` is what makes a partition that stops being assigned *disappear* from
  * the exposition rather than freezing at its last value — a frozen gauge is indistinguishable from a stuck consumer.
  */
final class ConsumerLagGauge(registry: MeterRegistry, groupId: String):

  private val gauge: MultiGauge = MultiGauge.builder(Meters.ConsumerLag).register(registry)

  /** Replaces the published series with `lags`. */
  def update(lags: Map[TopicPartition, Long]): Unit =
    val rows = lags.toVector
      .sortBy((partition, _) => (partition.topic, partition.partition))
      .map: (partition, lag) =>
        MultiGauge.Row.of(
          Tags.of(
            Meters.TagKeys.Group,
            groupId,
            Meters.TagKeys.Topic,
            partition.topic,
            Meters.TagKeys.Partition,
            partition.partition.toString
          ),
          java.lang.Long.valueOf(lag)
        )
    gauge.register(rows.asJava, true)

/** Reads committed and log-end offsets for one consumer group.
  *
  * Split from [[ConsumerLagGauge]] so the gauge can be exercised with hand-written maps, and split from the scheduler
  * so a failed admin call is a returned failure rather than an exception on a timer thread nobody is watching.
  */
final class AdminOffsets(admin: Admin, timeout: FiniteDuration):

  /** Committed offsets for `groupId`. A partition the group has an entry but no offset for is dropped — see
    * [[ConsumerLag.lags]] for why "unknown" must not become a number.
    */
  def committed(groupId: String): Map[TopicPartition, Long] =
    admin
      .listConsumerGroupOffsets(groupId)
      .partitionsToOffsetAndMetadata()
      .get(timeout.toMillis, TimeUnit.MILLISECONDS)
      .asScala
      .toMap
      .flatMap((partition, metadata) => Option(metadata).map(m => partition -> m.offset))

  /** Log-end offsets for exactly the partitions asked for. `LATEST` is the offset of the *next* record to be written,
    * which is what makes `end - committed` a record count rather than a count off by one.
    */
  def logEnds(partitions: Set[TopicPartition]): Map[TopicPartition, Long] =
    if partitions.isEmpty then Map.empty
    else
      admin
        .listOffsets(partitions.iterator.map(partition => partition -> OffsetSpec.latest()).toMap.asJava)
        .all()
        .get(timeout.toMillis, TimeUnit.MILLISECONDS)
        .asScala
        .toMap
        .map((partition, info) => partition -> info.offset)

  /** Asks the cluster who its brokers are. Cheap, and the only reachability question readiness needs answered. */
  def clusterNodes(): Int =
    admin.describeCluster().nodes().get(timeout.toMillis, TimeUnit.MILLISECONDS).size
