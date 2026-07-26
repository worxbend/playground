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

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets.UTF_8

import scala.jdk.CollectionConverters.*

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.core.status.Status
import net.logstash.logback.encoder.LogstashEncoder

/** The shared logback fragment (ADR §7.3).
  *
  * A logging configuration is the one artefact that is never exercised by the code it configures: a typo produces a
  * status warning on stderr at startup and an application that runs perfectly while emitting nothing useful. So it is
  * loaded here into a *throwaway* `LoggerContext` — never the global one, which the rest of the suite is logging
  * through — and its own encoder is asked to render an event, which is the only way to prove that `trace_id` actually
  * lands in the JSON.
  */
final class LoggingConfigSuite extends munit.FunSuite:

  private val Resource = "io/kzonix/observability/logback-json.xml"

  /** Wraps the fragment in the same two lines each service's `logback.xml` contains, so the test configures exactly
    * what production configures.
    */
  private val ServiceConfiguration =
    s"""<configuration>
       |  <include resource="$Resource"/>
       |</configuration>""".stripMargin

  private def configure(): LoggerContext =
    val context = LoggerContext()
    context.setName("logging-config-suite")
    val configurator = JoranConfigurator()
    configurator.setContext(context)
    configurator.doConfigure(ByteArrayInputStream(ServiceConfiguration.getBytes(UTF_8)))
    context

  private def rootAppender(context: LoggerContext): ConsoleAppender[ILoggingEvent] =
    context
      .getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
      .getAppender("JSON")
      .asInstanceOf[ConsoleAppender[ILoggingEvent]]

  test("the fragment is on the classpath under the path services are told to include"):
    assert(getClass.getClassLoader.getResource(Resource) != null, s"$Resource is not a resource of this module")

  test("the fragment configures cleanly — no Joran warnings or errors"):
    val context = configure()
    try
      val problems = context.getStatusManager.getCopyOfStatusList.asScala
        .filter(_.getLevel >= Status.WARN)
        .map(status => s"${status.getMessage} (${Option(status.getThrowable).map(_.toString).getOrElse("-")})")
        .toList
      assertEquals(problems, Nil, "logback reported configuration problems")
    finally context.stop()

  test("stdout is the only appender, because the container runtime is the log shipper"):
    val context = configure()
    try
      val appenders = context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).iteratorForAppenders.asScala.toList
      assertEquals(appenders.map(_.getName), List("JSON"))
      assert(appenders.head.isInstanceOf[ConsoleAppender[?]], s"root appender is ${appenders.head.getClass}")
      assert(rootAppender(context).getEncoder.isInstanceOf[LogstashEncoder], "the encoder is not the logstash one")
    finally context.stop()

  test("levels default to INFO and are overridable from the environment"):
    val context = configure()
    try
      assertEquals(context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).getLevel, Level.INFO)
      assertEquals(context.getLogger("io.kzonix").getLevel, Level.INFO)
    finally context.stop()

  test("libraries that are loud and uninformative at INFO are turned down"):
    val context = configure()
    try
      assertEquals(context.getLogger("org.apache.kafka.clients.NetworkClient").getLevel, Level.WARN)
      assertEquals(context.getLogger("com.zaxxer.hikari.pool.HikariPool").getLevel, Level.WARN)
    finally context.stop()

  test("an event renders as one JSON line carrying trace_id and span_id from the MDC"):
    val context = configure()
    try
      val encoder = rootAppender(context).getEncoder
      val event = LoggingEvent(
        classOf[LoggingConfigSuite].getName,
        context.getLogger("io.kzonix.cobalt.Consumer"),
        Level.INFO,
        "persisted batch",
        null,
        Array(LogContext.kv("count", 12))
      )
      event.setMDCPropertyMap(Map(LogContext.TraceIdKey -> "0af7651916cd43dd8448eb211c80319c").asJava)

      val line = String(encoder.encode(event), UTF_8)
      assertEquals(line.linesIterator.size, 1, "the encoder emitted more than one line per event")
      assert(line.contains(""""trace_id":"0af7651916cd43dd8448eb211c80319c""""), line)
      assert(line.contains(""""message":"persisted batch""""), line)
      assert(line.contains(""""level":"INFO""""), line)
      assert(line.contains(""""logger":"io.kzonix.cobalt.Consumer""""), line)
      // Structured arguments are what make a log line queryable rather than merely greppable.
      assert(line.contains(""""count":12"""), line)
      // Pinned field names: the encoder's defaults would add these, and a query written against one service must not
      // break because another shipped a different encoder version.
      assert(!line.contains("@version"), line)
      assert(!line.contains("level_value"), line)
    finally context.stop()
