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

import com.worxbend.kernel.event.Topics
import com.worxbend.persistence.DatabaseConfig
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

  test("maintenance is on by default, because forgetting it costs an outage at a month boundary"):
    // A partition job nobody enabled produces no error, no failed probe and no Kafka symptom — right up to the first
    // of the month, when every insert starts failing with "no partition of relation found for row".
    assert(config.maintenance.enabled, "database maintenance must be on unless a deployment turns it off")
    assertEquals(config.maintenance.monthsAhead, 3, "ADR §5 says N+3")

  test("retention is absent by default: detaching is configured, never defaulted"):
    // Detaching is the one operation in this service that makes data vanish from queries. It happens because an
    // operator asked for it.
    assertEquals(config.maintenance.retainMonths, None)
    assertEquals(config.maintenance.policy.retainMonths, None)

  test("the refresh interval is the staleness floor for every dashboard, and the budget the refresh must fit in"):
    // REFRESH ... CONCURRENTLY is a full recompute, never incremental: when its duration approaches this value the
    // materialized view is finished and ADR §12.4's incremental merge tables are the replacement.
    assert(config.maintenance.refreshInterval <= 15.minutes, "a dashboard this stale is a dashboard nobody trusts")
    assert(
      config.maintenance.partitionInterval > config.maintenance.refreshInterval,
      "months are long; there is nothing to gain from re-checking the calendar as often as the rollup"
    )

  test("the detach lock timeout is short, because waiting for ACCESS EXCLUSIVE queues every ingest insert behind it"):
    assert(config.maintenance.detachLockTimeout <= 30.seconds)
    // Constructing the policy is what would reject a nonsensical combination, so it is exercised here rather than
    // discovered at boot.
    assertEquals(config.maintenance.policy.monthsAhead, config.maintenance.monthsAhead)

  test("admin authentication is on by default, and the shipped configuration carries no key"):
    // Both halves matter. A security layer whose default is "off" ships off — nothing fails when it is, so the first
    // evidence is somebody else's POST /admin/consumer:restart. And with no key default, a deployment that leaves
    // ADMIN_AUTH_SECRET unset does not quietly accept every token: it refuses to boot, naming the field.
    assert(config.auth.enabled, "the admin surface must be authenticated unless a deployment says otherwise")
    assertEquals(config.auth.secret, None)
    assertEquals(config.auth.publicKey, None)
    val problem = JwtVerifier.from(config.auth).swap.getOrElse(fail("a keyless verifier was accepted"))
    assert(problem.contains("cobalt.auth.secret"), problem)

  test("the two scopes are distinct, and neither is wolfram's ingestion scope"):
    // An operator with one issuer may give both services the same signing key. A producer token must still not open
    // the door that skips unconsumed events.
    assertEquals(config.auth.scopeFor(AdminScope.Read), Some("admin:read"))
    assertEquals(config.auth.scopeFor(AdminScope.Write), Some("admin:write"))
    assertNotEquals(config.auth.scopeFor(AdminScope.Read), config.auth.scopeFor(AdminScope.Write))
    assert(!Set(config.auth.readScope, config.auth.writeScope).contains("events:write"))

  test("the auth leeway is a skew tolerance, not an amnesty"):
    // This is the window in which a revoked token still works, and there is no revocation list anywhere in this system.
    assert(config.auth.leeway <= 60.seconds, s"${config.auth.leeway} is a long time to honour a withdrawn credential")

  test("the database namespace resolves from modules/persistence's reference.conf"):
    val database = DatabaseConfig
      .load(ConfigSource.default)
      .fold(failures => fail(s"the database configuration does not parse: ${failures.prettyPrint()}"), identity)
    assert(database.write.maximumPoolSize > 0)
    assert(!database.write.readOnly, "cobalt is the write side")
