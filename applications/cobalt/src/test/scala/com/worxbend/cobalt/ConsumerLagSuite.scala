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

import com.worxbend.observability.Meters
import org.apache.kafka.common.TopicPartition

/** Lag arithmetic and its exposition.
  *
  * Lag is the number an alert fires on, so the cases that matter are the ones that would fire it spuriously: a group
  * that has never committed, and a committed offset briefly ahead of the log end.
  */
final class ConsumerLagSuite extends munit.FunSuite:

  private def partition(n: Int): TopicPartition = TopicPartition(Fixtures.Topic, n)

  test("lag is the distance from the committed offset to the log end"):
    val lags = ConsumerLag.lags(
      committed = Map(partition(0) -> 100L, partition(1) -> 40L),
      logEnds = Map(partition(0) -> 130L, partition(1) -> 40L)
    )
    assertEquals(lags(partition(0)), 30L)
    assertEquals(lags(partition(1)), 0L, "a caught-up partition lags by zero, not by one")

  test("a partition the group has never committed to is omitted rather than invented"):
    val lags = ConsumerLag.lags(committed = Map.empty, logEnds = Map(partition(0) -> 900L))
    assertEquals(lags, Map.empty[TopicPartition, Long], "reporting 900 here would page on every new topic")

  test("a committed offset ahead of the log end clamps to zero"):
    val lags = ConsumerLag.lags(Map(partition(0) -> 120L), Map(partition(0) -> 100L))
    assertEquals(lags(partition(0)), 0L, "a negative lag reads as a broken exporter, not as a truncation")

  test("a partition with no known log end is omitted"):
    val lags = ConsumerLag.lags(Map(partition(0) -> 5L, partition(1) -> 7L), Map(partition(0) -> 9L))
    assertEquals(lags.keySet, Set(partition(0)))

  test("the total is the sum across partitions"):
    assertEquals(ConsumerLag.total(Map(partition(0) -> 3L, partition(1) -> 4L)), 7L)
    assertEquals(ConsumerLag.total(Map.empty), 0L)

  test("the gauge publishes one series per partition, tagged group/topic/partition"):
    val telemetry = Fixtures.telemetry()
    try
      val gauge = ConsumerLagGauge(telemetry.registry, Fixtures.GroupId)
      gauge.update(Map(partition(0) -> 12L, partition(3) -> 0L))
      val gauges = telemetry.registry.find(Meters.ConsumerLag).gauges()
      assertEquals(gauges.size, 2)
      val value = telemetry.registry
        .find(Meters.ConsumerLag)
        .tag(Meters.TagKeys.Partition, "0")
        .tag(Meters.TagKeys.Group, Fixtures.GroupId)
        .tag(Meters.TagKeys.Topic, Fixtures.Topic)
        .gauge()
      assertEquals(Option(value).map(_.value()), Some(12.0d))
    finally telemetry.close()

  test("a partition that stops being assigned disappears instead of freezing at its last value"):
    val telemetry = Fixtures.telemetry()
    try
      val gauge = ConsumerLagGauge(telemetry.registry, Fixtures.GroupId)
      gauge.update(Map(partition(0) -> 5L, partition(1) -> 5L))
      gauge.update(Map(partition(0) -> 6L))
      val remaining = telemetry.registry.find(Meters.ConsumerLag).gauges()
      assertEquals(remaining.size, 1, "a frozen gauge is indistinguishable from a stuck consumer")
    finally telemetry.close()
