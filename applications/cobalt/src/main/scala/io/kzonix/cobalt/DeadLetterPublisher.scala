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

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.control.NonFatal

import com.typesafe.scalalogging.StrictLogging
import io.kzonix.eventing.DeadLetter
import io.kzonix.eventing.KafkaCodecs
import io.kzonix.kernel.event.Topics
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer

/** Where a record goes when the consumer gives up on it.
  *
  * A trait rather than a concrete producer for one reason that matters: this is the only side effect in the batch path
  * that is not the database, and the unit tests of [[BatchProcessor]] are exactly the tests that need to assert *what*
  * was dead-lettered without a broker in the room.
  */
trait DeadLetterPublisher:

  /** Publishes one dead letter. The `Future` fails only if the record could not be made durable — see
    * [[BatchProcessor]] for why that failure must not be swallowed.
    */
  def publish(deadLetter: DeadLetter): Future[Unit]

  def close(): Unit

/** The real one: a plain `KafkaProducer` writing structured-mode CloudEvents to the DLQ topic.
  *
  * **No Pekko producer sink here, on purpose.** The DLQ write is a *dependency of one batch's completion*, not a stream
  * of its own: the offset of a poison record may only be committed once its dead letter is durable, so the write has to
  * be something the batch can `flatMap` on. Splicing a `Producer.flow` into the graph would either reorder that
  * dependency or need a second commit path, and both are ways to lose a poison record while committing past it.
  *
  * **`acks=all` and idempotence come from [[KafkaCodecs.producerConfig]]** and cannot be turned off by configuration.
  * A DLQ producer that acknowledges before the record is replicated makes the dead letter *less* durable than the
  * record it replaces, which defeats the entire mechanism.
  */
final class KafkaDeadLetterPublisher(
  producer: Producer[String, Array[Byte]],
  topic: String,
  closeTimeout: java.time.Duration
) extends DeadLetterPublisher
    with StrictLogging:

  def publish(deadLetter: DeadLetter): Future[Unit] =
    KafkaCodecs.deadLetterRecord(deadLetter, topic) match
      case Left(reason) =>
        // The dead letter is built by this build from bytes it already holds, so a failure here is a bug in the DLQ
        // envelope rather than anything the producer did. Failing the future keeps the offset uncommitted, which is
        // the conservative direction: the record is replayed rather than silently dropped.
        logger.error(s"could not encode a dead letter for topic $topic: $reason")
        Future.failed(IllegalStateException(s"dead letter could not be encoded: $reason"))
      case Right(record) =>
        val promise = Promise[Unit]()
        try
          val _ = producer.send(
            record,
            (_, error) =>
              if error == null then
                val _ = promise.success(())
              else
                val _ = promise.failure(error)
          )
          promise.future
        catch
          // send() throws synchronously on metadata timeout, a full accumulator, or serialization failure. All three
          // mean "not durable", so they answer exactly like a broker refusal.
          case NonFatal(error) => Future.failed(error)

  def close(): Unit =
    try producer.flush()
    catch case NonFatal(error) => logger.warn("flushing the dead-letter producer during shutdown failed", error)
    producer.close(closeTimeout)

object KafkaDeadLetterPublisher:

  /** Builds the production publisher.
    *
    * The serializers are `String`/`Array[Byte]` and never `io.cloudevents.kafka.CloudEventSerializer`: the record is
    * already fully formed by `KafkaCodecs.deadLetterRecord`, which writes structured mode itself. Letting the SDK
    * re-encode would reintroduce the RFC 3339 rendering defect `modules/eventing` exists to avoid.
    */
  def start(config: ConsumerConfig): KafkaDeadLetterPublisher =
    val settings = config.properties ++ Map("bootstrap.servers" -> config.bootstrapServers)
    val producer =
      KafkaProducer[String, Array[Byte]](
        KafkaCodecs.producerConfig(settings),
        StringSerializer(),
        ByteArraySerializer()
      )
    KafkaDeadLetterPublisher(producer, config.dlqTopic, java.time.Duration.ofMillis(config.drainTimeout.toMillis))

  /** The topic a dead letter goes to when nothing overrides it: kernel's constant, so cobalt and any replayer
    * agree on the name.
    */
  val DefaultTopic: String = Topics.CloudEventsDlq
