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

package io.kzonix.eventing

import java.net.URI

import io.cloudevents.core.builder.CloudEventBuilder
import io.cloudevents.kafka.KafkaMessageFactory
import io.cloudevents.kafka.impl.KafkaHeaders
import munit.FunSuite
import org.apache.kafka.common.header.internals.RecordHeaders

/** The drift guard for [[CloudEventHeaders]].
  *
  * That object re-declares the binding's header vocabulary because half of the SDK's own copy is `protected` and in an
  * `impl` package. Re-declaring is only safe if the two cannot silently disagree, so this suite checks them against
  * each other — directly where the SDK's constant is public, and otherwise by having the SDK's *writer* produce a
  * record and comparing the header names it chose with the ones this module writes. Deleting this suite turns those
  * strings back into a guess that fails at runtime, on a real broker, as an empty consumer.
  */
class CloudEventHeadersSuite extends FunSuite:

  test("the public SDK constants match"):
    assertEquals(CloudEventHeaders.ContentType, KafkaHeaders.CONTENT_TYPE)
    assertEquals(CloudEventHeaders.SpecVersion, KafkaHeaders.SPEC_VERSION)

  test("the ce_ prefix matches the one the SDK's own writer uses"):
    val event = CloudEventBuilder
      .v1()
      .withId("event-1")
      .withSource(URI("/sensors/kitchen"))
      .withType("io.kzonix.iot.telemetry")
      .withSubject("kitchen-1")
      .withDataContentType("application/json")
      .withExtension("tenantid", "acme")
      .build()

    val sdkRecord = KafkaMessageFactory.createWriter[String]("t", "k").writeBinary(event)
    val ours = RecordHeaders()
    val envelope = CloudEventAdapter.toEnvelope(event)
    assert(envelope.isRight, envelope)
    val _ = envelope.map(e => ContentMode.write(ContentMode.Binary, e, ours))

    assertEquals(CloudEventHeaders.keys(ours).toSet, CloudEventHeaders.keys(sdkRecord.headers).toSet)

  test("get returns the last value, which is what a re-injected trace header relies on"):
    val headers = RecordHeaders()
    val _ = headers.add("k", "first".getBytes("UTF-8"))
    val _ = headers.add("k", "second".getBytes("UTF-8"))
    assertEquals(CloudEventHeaders.get(headers, "k"), Some("second"))
    assertEquals(CloudEventHeaders.keys(headers).toSeq, Seq("k"))

  test("put replaces rather than appends, so repeated writes cannot accumulate"):
    val headers = RecordHeaders()
    val _ = CloudEventHeaders.put(headers, "k", "first")
    val _ = CloudEventHeaders.put(headers, "k", "second")
    assertEquals(headers.headers("k").iterator.hasNext, true)
    assertEquals(CloudEventHeaders.toMap(headers), Map("k" -> "second"))

  test("attribute and attributeOf are inverses over the ce_ prefix"):
    assertEquals(CloudEventHeaders.attributeOf(CloudEventHeaders.attribute("type")), Some("type"))
    assertEquals(CloudEventHeaders.attributeOf("content-type"), None)
