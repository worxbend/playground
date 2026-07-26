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

package io.kzonix.observability

/** Process identity resolution.
  *
  * Worth testing despite being a handful of `getOrElse`s: this is the code that decides what a metric's `service` tag
  * and a trace's `service.name` say, and a wrong answer here does not fail — it produces a dashboard that is subtly
  * about the wrong thing.
  */
final class TelemetryConfigSuite extends munit.FunSuite:

  private def env(pairs: (String, String)*): String => Option[String] = pairs.toMap.get

  test("version and instance are read from the environment"):
    val config = TelemetryConfig.fromEnv(
      "cobalt",
      env(TelemetryConfig.VersionEnv -> "1.4.2", TelemetryConfig.InstanceEnv -> "cobalt-7f9c-x2")
    )
    assertEquals(config, TelemetryConfig("cobalt", "1.4.2", "cobalt-7f9c-x2"))

  test("a missing version is an obviously-wrong value, not an empty tag"):
    // An empty tag reads as a rendering glitch; "0.0.0-unknown" in a legend reads as a bug report.
    val config = TelemetryConfig.fromEnv("wolfram", env(TelemetryConfig.InstanceEnv -> "host-1"))
    assertEquals(config.serviceVersion, TelemetryConfig.UnknownVersion)

  test("an empty env value is treated as absent"):
    val config = TelemetryConfig.fromEnv("wolfram", env(TelemetryConfig.VersionEnv -> ""))
    assertEquals(config.serviceVersion, TelemetryConfig.UnknownVersion)

  test("a missing HOSTNAME falls back to the host name rather than failing startup"):
    val config = TelemetryConfig.fromEnv("ferrite", _ => None)
    assert(config.instanceId.nonEmpty, "instance id must never be empty")

  test("the service name comes from the caller, never from the environment"):
    // Two replicas of one service reporting under different names is unfixable after the fact; making the name a
    // parameter of `main` removes the possibility.
    assertEquals(TelemetryConfig.fromEnv("ferrite", env("SERVICE_NAME" -> "not-this")).serviceName, "ferrite")

  test("resource attributes carry version and instance in the OTel spelling"):
    assertEquals(
      TelemetryConfig("cobalt", "1.4.2", "cobalt-7f9c-x2").resourceAttributes,
      "service.version=1.4.2,service.instance.id=cobalt-7f9c-x2"
    )
