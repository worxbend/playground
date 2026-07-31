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

import com.typesafe.scalalogging.StrictLogging
import com.worxbend.eventing.KafkaCodecs
import java.time.Duration as JavaDuration
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Offsets for one DLQ partition. */
final case class DlqPartitionDepth(partition: Int, earliest: Long, latest: Long):

  /** Records between the earliest retained offset and the log end. */
  def records: Long = math.max(0L, latest - earliest)

/** How much is on the DLQ right now.
  *
  * **An upper bound, and named as one.** `latest - earliest` counts offsets, not distinct dead letters: the DLQ is
  * keyed on origin coordinates so a compacted topic can hold fewer live records than its offset range suggests, and a
  * transactional marker occupies an offset without being a record at all. Reporting it as exact would be the more
  * comfortable lie; what an operator needs from this number is "is the DLQ empty, a handful, or a flood", and an upper
  * bound answers all three without pretending to a precision the log cannot give.
  */
final case class DlqDepth(topic: String, partitions: Vector[DlqPartitionDepth]):

  def outstanding: Long = partitions.map(_.records).sum

/** Read and write access to the DLQ topic, as the two operations an operator needs and nothing else.
  *
  * A trait because [[DeadLetterAdmin]]'s decisions — bounding, refusal, the dry-run split, the meters — are worth
  * testing without a broker, and because the Kafka implementation is a lock around two clients that a unit test would
  * learn nothing from.
  */
trait DeadLetterStore:

  /** Offsets per partition. Two admin-style round trips; no records are fetched. */
  def depth(): DlqDepth

  /** The most recent dead letters, newest first, at most `limit` of them. */
  def recent(limit: Int): Vector[DlqRecord]

  /** Publishes one record and blocks until the broker acknowledges it. Throws if it does not. */
  def publish(record: ProducerRecord[String, Array[Byte]]): Unit

  def close(): Unit

/** The real one: one `KafkaConsumer` and one `KafkaProducer`, serialised behind a single lock.
  *
  * **The lock is a feature, not an implementation detail.** `KafkaConsumer` is not thread-safe and Undertow hands every
  * request to a different worker thread, so some serialisation is mandatory; making it one lock over the *whole* store
  * additionally means two operators cannot replay at the same time. A replay is a deliberate burst of produces at a
  * broker that is, by the nature of the situation, already having a bad day — two of them interleaved is how a bad
  * moment becomes an outage, and the second operator waiting is the correct outcome.
  *
  * **`assign`, never `subscribe`.** This consumer joins no group: it seeks to explicit offsets and reads a bounded
  * range. Subscribing would give it group membership, a rebalance it can trigger by being slow, and — worst — committed
  * offsets that could be confused with the real consumer's. Reading the DLQ must not be able to move anything.
  *
  * **Its own producer, not the dead-letter publisher's.** Sharing one would tie two lifetimes together for the sake of
  * one TCP connection, and the failure mode of getting that wrong (a replay against a producer the shutdown sequence
  * has already closed) is silent. Each owner closes what it opened.
  */
final class KafkaDeadLetterStore(
  consumer: Consumer[String, Array[Byte]],
  producer: Producer[String, Array[Byte]],
  topic: String,
  pollTimeout: FiniteDuration,
  closeTimeout: JavaDuration
) extends DeadLetterStore
    with StrictLogging:

  private val lock: Object = Object()

  /** How long one `poll` may block. Short relative to [[pollTimeout]], which bounds the whole read, so a partition that
    * has nothing left to give costs a fraction of the budget rather than all of it.
    */
  private val pollSlice: JavaDuration = JavaDuration.ofMillis(math.min(500L, pollTimeout.toMillis).max(1L))

  def depth(): DlqDepth = lock.synchronized:
    val partitions = assignable()
    if partitions.isEmpty then DlqDepth(topic, Vector.empty)
    else
      val earliest = consumer.beginningOffsets(partitions.asJava).asScala
      val latest = consumer.endOffsets(partitions.asJava).asScala
      DlqDepth(
        topic,
        partitions.toVector
          .sortBy(_.partition)
          .map: partition =>
            DlqPartitionDepth(
              partition.partition,
              earliest.get(partition).fold(0L)(_.longValue),
              latest.get(partition).fold(0L)(_.longValue)
            )
      )

  /** Reads the tail of every partition and returns the newest `limit` across all of them.
    *
    * **Bounded three ways, because an unbounded listing endpoint on a topic is a way to OOM the service that is
    * supposed to be telling you it is unhealthy.** The seek is `max(earliest, latest - limit)` per partition, so at
    * most `limit × partitions` records are fetched however large the topic is; the loop stops at the log end it
    * computed before it started, so a producer writing concurrently cannot extend the read; and the whole thing gives
    * up after [[pollTimeout]] with whatever it has, because a listing that hangs is worse than a listing that is short.
    *
    * Reading `limit` from *each* partition and then taking `limit` overall is deliberate over-reading: with three DLQ
    * partitions there is no way to know which holds the newest records without looking, and the alternative —
    * `limit / partitions` from each — silently returns the wrong set whenever the dead letters are skewed to one
    * partition, which is the normal case when one device is producing all of them.
    */
  def recent(limit: Int): Vector[DlqRecord] = lock.synchronized:
    val partitions = assignable()
    if partitions.isEmpty || limit <= 0 then Vector.empty
    else
      consumer.assign(partitions.asJava)
      try
        val earliest = consumer.beginningOffsets(partitions.asJava).asScala
        val latest = consumer.endOffsets(partitions.asJava).asScala
        val starts = partitions.iterator.map { partition =>
          val end = latest.get(partition).fold(0L)(_.longValue)
          val begin = earliest.get(partition).fold(0L)(_.longValue)
          partition -> math.max(begin, end - limit)
        }.toMap
        val expected = partitions.iterator.map(p => latest.get(p).fold(0L)(_.longValue) - starts(p)).sum
        partitions.foreach(partition => consumer.seek(partition, starts(partition)))
        drain(expected)
          .map(read)
          .sortBy(record => (record.timestamp.getOrElse(-1L), record.partition, record.offset))
          .reverse
          .take(limit)
      finally consumer.assign(Set.empty[TopicPartition].asJava)

  /** One produce, acknowledged before the call returns.
    *
    * **Sequential by construction, for the same reason [[BatchProcessor]]'s dead-letter writes are.** Firing a fan-out
    * of produces at a broker that may itself be why these records died is how a poison batch becomes an outage; and a
    * replay whose records are acknowledged one at a time is a replay whose partial failure has an exact boundary, which
    * is what makes "published 37 of 50, stopped at ref X" a statement rather than a guess.
    */
  def publish(record: ProducerRecord[String, Array[Byte]]): Unit = lock.synchronized:
    val _ = producer.send(record).get(closeTimeout.toMillis, TimeUnit.MILLISECONDS)

  def close(): Unit =
    lock.synchronized:
      try consumer.close(closeTimeout)
      catch case NonFatal(error) => logger.warn("closing the DLQ reader failed", error)
      try producer.close(closeTimeout)
      catch case NonFatal(error) => logger.warn("closing the DLQ replay producer failed", error)

  /** The partitions of the DLQ topic, or empty if the topic does not exist.
    *
    * Empty rather than an exception: a DLQ topic that has never been created is a deployment fact an operator should
    * read off the response (`"partitions": []`), not a 500 that looks like the admin surface is broken.
    */
  private def assignable(): Set[TopicPartition] =
    Option(consumer.partitionsFor(topic, pollSlice)).toVector.flatMap(_.asScala).map { info =>
      TopicPartition(topic, info.partition)
    }.toSet

  /** Polls until `expected` records have been collected, the budget runs out, or the log stops giving.
    *
    * The empty-poll cut-off is not belt-and-braces: `expected` is derived from the offset range, and under compaction
    * or retention some of those offsets no longer hold a record, so the count is reachable only on a topic that has
    * neither. Without it, every listing on a compacted DLQ would burn the full timeout.
    */
  private def drain(expected: Long): Vector[ConsumerRecord[String, Array[Byte]]] =
    val deadline = System.nanoTime() + pollTimeout.toNanos
    val collected = Vector.newBuilder[ConsumerRecord[String, Array[Byte]]]
    var seen = 0L
    var idle = 0
    while seen < expected && idle < KafkaDeadLetterStore.IdlePolls && System.nanoTime() < deadline do
      val batch = consumer.poll(pollSlice)
      if batch.isEmpty then idle += 1
      else
        idle = 0
        batch.iterator.asScala.foreach: record =>
          collected += record
          seen += 1
    collected.result()

  private def read(record: ConsumerRecord[String, Array[Byte]]): DlqRecord =
    DlqRecord(
      partition = record.partition,
      offset = record.offset,
      timestamp = Option(record.timestamp).filter(_ >= 0L).map(_.longValue),
      key = Option(record.key),
      entry = DeadLetterReplay.readDeadLetter(record.headers, Option(record.value))
    )

object KafkaDeadLetterStore:

  /** Consecutive empty polls that end a read. Three, so one slow fetch does not truncate a listing. */
  val IdlePolls: Int = 3

  /** Builds the production store.
    *
    * The consumer settings are the ones that make this client incapable of affecting the pipeline it is inspecting: no
    * `group.id`, so it has no group and no committed offsets; `enable.auto.commit=false` for the same reason twice
    * over; and `auto.offset.reset=none`, because every read seeks explicitly and a reset would mean this client had
    * silently started somewhere other than where it was told to.
    *
    * `max.poll.records` is pinned to the configured ceiling so one poll can never return more records than the whole
    * request is allowed to touch.
    */
  def start(consumer: ConsumerConfig, replay: ReplayConfig): KafkaDeadLetterStore =
    val common = consumer.properties ++ Map("bootstrap.servers" -> consumer.bootstrapServers)
    val reader = KafkaConsumer[String, Array[Byte]](
      (common ++ Map(
        "client.id" -> s"${consumer.groupId}-dlq-reader",
        "enable.auto.commit" -> "false",
        "auto.offset.reset" -> "none",
        "max.poll.records" -> replay.maxRecords.toString
      )).view.mapValues(value => value: AnyRef).toMap.asJava,
      StringDeserializer(),
      ByteArrayDeserializer()
    )
    val writer = KafkaProducer[String, Array[Byte]](
      KafkaCodecs.producerConfig(common ++ Map("client.id" -> s"${consumer.groupId}-dlq-replay")),
      StringSerializer(),
      ByteArraySerializer()
    )
    KafkaDeadLetterStore(
      reader,
      writer,
      consumer.dlqTopic,
      replay.pollTimeout,
      JavaDuration.ofMillis(consumer.drainTimeout.toMillis)
    )
