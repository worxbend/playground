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

import io.kzonix.kernel.event.Topics
import io.kzonix.persistence.DatabaseConfig
import pureconfig.ConfigSource
import scala.concurrent.duration.DurationInt

/** The shipped `application.conf`, parsed.
  *
  * A consumer whose topic name has a typo starts cleanly and receives nothing, which is the least debuggable failure in
  * the system (kernel's `Topics` says so in as many words). Asserting the shipped defaults against kernel's constants
  * is what turns that into a failing test.
  */
final class CobaltConfigSuite extends munit.FunSuite:

  private val config: CobaltConfig =
    CobaltConfig
      .load()
      .fold(failures => fail(s"the shipped configuration does not parse: ${failures.prettyPrint()}"), identity)

  test("the topics are kernel's constants and not re-typed strings"):
    assertEquals(config.consumer.topic, Topics.CloudEvents)
    assertEquals(config.consumer.dlqTopic, Topics.CloudEventsDlq)

  test("batching matches ADR §4.3"):
    assertEquals(config.consumer.batchSize, 500)
    assertEquals(config.consumer.batchWindow, 250.millis)

  test("the restart budget is bounded"):
    assertEquals(config.restart.maxRestarts, 50)
    assertEquals(config.restart.maxRestartsWithin, 10.minutes)
    assert(config.restart.randomFactor > 0.0d, "without jitter every replica retries in lockstep")

  test("lag polling is measured in tens of seconds, not in seconds"):
    assert(config.lag.refreshInterval >= 15.seconds)
    assert(config.lag.requestTimeout < config.lag.refreshInterval)

  test("the database namespace resolves from modules/persistence's reference.conf"):
    val database = DatabaseConfig
      .load(ConfigSource.default)
      .fold(failures => fail(s"the database configuration does not parse: ${failures.prettyPrint()}"), identity)
    assert(database.write.maximumPoolSize > 0)
    assert(!database.write.readOnly, "cobalt is the write side")
