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

import scala.collection.mutable.ListBuffer

/** The contention rule, without a database.
  *
  * `AdvisoryLock.guarded` is three lines and every one of them is a way to take the whole fleet down quietly. A body
  * that runs anyway makes the lock decorative — N replicas issue the same DDL and the losers fail on SQLSTATEs the
  * job then has to interpret. A lock that is not released when the body throws is worse: it survives on a *pooled*
  * connection until that connection is retired (25 minutes, here), during which every replica skips every cycle,
  * every metric reads `skipped`, and nothing anywhere reads as broken. A release without an acquire is worse again,
  * because in a reentrant nesting it drops a lock somebody else still believes they hold.
  *
  * All four are asserted below with recording fakes, because none of them can be observed from the outside of a real
  * database without two threads and a sleep.
  */
final class AdvisoryLockSuite extends munit.FunSuite:

  private final class Recorder(granted: Boolean):
    val events: ListBuffer[String] = ListBuffer.empty
    def acquire(): Boolean =
      events += "acquire"
      granted
    def release(): Unit =
      events += "release"
      ()
    def ran(): Unit =
      events += "body"
      ()

  test("the body runs under the lock and the lock is released afterwards"):
    val recorder = Recorder(granted = true)
    val result = AdvisoryLock.guarded(() => recorder.acquire(), () => recorder.release()):
      recorder.ran()
      42
    assertEquals(result, Some(42))
    assertEquals(recorder.events.toList, List("acquire", "body", "release"))

  test("a replica that loses the race skips: the body does not run at all"):
    // Not "runs and fails", not "waits" — the whole point of pg_try_advisory_lock over pg_advisory_lock is that the
    // loser does no work and holds nothing.
    val recorder = Recorder(granted = false)
    val result = AdvisoryLock.guarded(() => recorder.acquire(), () => recorder.release()):
      recorder.ran()
      42
    assertEquals(result, None)
    assert(!recorder.events.contains("body"), "the body ran without the lock")

  test("a lock that was never acquired is never released"):
    // An unbalanced release is not harmless: PostgreSQL warns, and in a reentrant nesting it drops a lock the outer
    // caller still holds.
    val recorder = Recorder(granted = false)
    val _ = AdvisoryLock.guarded(() => recorder.acquire(), () => recorder.release())(())
    assertEquals(recorder.events.toList, List("acquire"))

  test("the lock is released when the body throws, and the body's exception is the one that escapes"):
    // The failure this prevents is the quiet one: a lock stranded on a pooled connection makes every replica skip
    // every cycle for as long as that connection lives, and every metric reads a perfectly healthy `skipped`.
    val recorder = Recorder(granted = true)
    val failure = intercept[IllegalStateException]:
      AdvisoryLock.guarded(() => recorder.acquire(), () => recorder.release()):
        recorder.ran()
        throw IllegalStateException("the partition job failed")
    assertEquals(failure.getMessage, "the partition job failed")
    assertEquals(recorder.events.toList, List("acquire", "body", "release"))

  test("acquisition is attempted exactly once — a try-lock that retried would be a blocking lock"):
    val recorder = Recorder(granted = false)
    val _ = AdvisoryLock.guarded(() => recorder.acquire(), () => recorder.release())(())
    assertEquals(recorder.events.count(_ == "acquire"), 1)

  test("the two jobs take different keys, so a long refresh cannot lock out partition creation"):
    // A missing partition is a hard ingest failure; a stale rollup is a slightly old dashboard. Sharing one key would
    // let the cheap-to-lose job block the expensive-to-lose one.
    assertNotEquals(AdvisoryLock.Partitions, AdvisoryLock.Rollup)
    assertEquals(AdvisoryLock.Partitions.namespace, AdvisoryLock.Rollup.namespace)

  test("both keys sit under one namespace, which is what separates them from another application's locks"):
    // Advisory locks are one cluster-wide space with no ownership: anything else picking these two integers would
    // silently serialise against these jobs.
    Vector(AdvisoryLock.Partitions, AdvisoryLock.Rollup).foreach: key =>
      assertEquals(key.namespace, AdvisoryLock.Namespace)
