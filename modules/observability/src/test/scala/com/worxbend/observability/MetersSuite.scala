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

package com.worxbend.observability

/** Invariants of the shared vocabulary.
  *
  * The point of `Meters` is that three services and one dashboard agree on strings. These tests guard the two ways that
  * agreement silently breaks: a name that Micrometer's Prometheus convention mangles into a different timeseries than
  * the author expected, and two constants that collide so an edit to one appears to have no effect.
  */
final class MetersSuite extends munit.FunSuite:

  /** Every meter name in the vocabulary, discovered rather than listed.
    *
    * **This used to be a hand-written list, and it was wrong.** Three meters had been added to `Meters` without being
    * added here — `consume.batch.latency`, `dlq.replay.operations`, `dlq.replay.records` — so the naming and collision
    * invariants below silently did not cover them. A list that must be kept in step with a set of constants is a list
    * that drifts, and the drift is invisible: the suite stays green, because it is still checking the names it knows.
    *
    * Reflection is the wrong tool almost everywhere and the right one here. `Meters` is a flat object of `String`
    * constants with no companion structure to fold over, and the alternative — a macro, or restructuring the vocabulary
    * into an enum — would change production code to serve a test. The predicate is narrow: a public no-arg method on
    * `Meters` returning `String`, minus the two that are URL paths and are asserted separately.
    */
  private val meterNames: List[String] =
    Meters.getClass.getDeclaredMethods.toList
      .filter(method => method.getParameterCount == 0 && method.getReturnType == classOf[String])
      .map(method => method.invoke(Meters).asInstanceOf[String])
      .filterNot(_.startsWith("/"))
      .sorted

  private val tagKeys: List[String] =
    List(
      Meters.TagKeys.Service,
      Meters.TagKeys.Version,
      Meters.TagKeys.Instance,
      Meters.TagKeys.EventType,
      Meters.TagKeys.Mode,
      Meters.TagKeys.Reason,
      Meters.TagKeys.Topic,
      Meters.TagKeys.Partition,
      Meters.TagKeys.Group,
      Meters.TagKeys.Shape,
      Meters.TagKeys.Route,
      Meters.TagKeys.Uri,
      Meters.TagKeys.Outcome,
      Meters.TagKeys.Job
    )

  test("meter names use Micrometer's dot convention, not the Prometheus spelling"):
    // Hard-coding underscores here would produce doubled names the day a second registry with a different naming
    // convention is added.
    meterNames.foreach: name =>
      assert(!name.contains("_"), s"$name is spelled for Prometheus, not for Micrometer")
      assert(name == name.toLowerCase, s"$name is not lower case")
      assert(name.matches("[a-z][a-z0-9.]*[a-z0-9]"), s"$name is not a valid dotted meter name")

  test("no two meter constants collide"):
    assertEquals(meterNames.distinct.size, meterNames.size, s"two meters share a name: $meterNames")

  test("the discovered vocabulary covers every meter the services emit"):
    // A guard on the guard: if the reflection above ever stops finding meters — a refactor to `def`s with arguments,
    // a move into a nested object — every invariant in this suite would pass vacuously over an empty list. Naming a
    // few known members keeps that failure loud. The count is a lower bound on purpose; adding a meter must not
    // require editing a test.
    assert(meterNames.sizeIs >= 20, s"only ${meterNames.size} meters discovered — has Meters been restructured?")
    List(
      Meters.IngestReceived,
      Meters.ConsumeBatchLatency,
      Meters.DlqReplayOperations,
      Meters.DlqReplayRecords,
      Meters.HttpServerRequests
    ).foreach(name => assert(meterNames.contains(name), s"$name was not discovered"))

  test("no two tag keys collide"):
    assertEquals(tagKeys.distinct.size, tagKeys.size, "two tag keys share a name")

  test("the uninstrumented paths are the scrape and health endpoints"):
    // Leaving these in http.server.requests inflates request rate and gives the scrape endpoint a timeseries
    // describing itself (ADR §7.1).
    assertEquals(Meters.UninstrumentedPaths, Set("/metrics", "/health"))

  test("closed tag-value sets stay closed"):
    assertEquals(Set(Meters.Modes.Binary, Meters.Modes.Structured).size, 2)
    assertEquals(
      Set(Meters.Outcomes.Success, Meters.Outcomes.Failure, Meters.Outcomes.Duplicate, Meters.Outcomes.Skipped).size,
      4
    )
    assertEquals(Set(Meters.Jobs.Partitions, Meters.Jobs.Rollup).size, 2)

  test("job names are stable kebab-case identifiers a dashboard can group by"):
    // Same rule as the reason values below, and for the same reason: these end up in a Prometheus label that a
    // recording rule and an alert both spell out by hand.
    List(Meters.Jobs.Partitions, Meters.Jobs.Rollup).foreach: job =>
      assert(job.matches("[a-z][a-z-]*[a-z]"), s"$job is not a stable kebab-case job name")

  test("reason values are operator-facing categories, not free text"):
    val reasons = List(
      Meters.Reasons.Malformed,
      Meters.Reasons.InvalidAttributes,
      Meters.Reasons.InvalidPayload,
      Meters.Reasons.UnknownType,
      Meters.Reasons.TooLarge,
      Meters.Reasons.Unpersistable
    )
    assertEquals(reasons.distinct.size, reasons.size)
    reasons.foreach: reason =>
      assert(reason.matches("[a-z][a-z-]*[a-z]"), s"$reason is not a stable kebab-case category")
