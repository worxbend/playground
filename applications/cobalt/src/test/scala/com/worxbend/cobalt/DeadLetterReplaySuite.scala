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

import com.worxbend.eventing.CloudEventHeaders
import com.worxbend.eventing.ContentMode
import com.worxbend.eventing.DecodeFailure
import com.worxbend.eventing.KafkaCodecs
import com.worxbend.kernel.event.Binary
import java.nio.charset.StandardCharsets.UTF_8
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.TimestampType
import scala.jdk.CollectionConverters.*

/** Every decision a replay makes, with no broker in the room.
  *
  * These are the assertions that must not need a container: which records are selected, in what order they go back, who
  * is refused, and — the one the whole feature rests on — that a rebuilt record is byte-for-byte the record that died.
  */
final class DeadLetterReplaySuite extends munit.FunSuite:

  private val MaxRecords = 200
  private val MaxAttempts = 3

  private def letter(id: String, partition: Int = 0, offset: Long = 0L) =
    Fixtures.deadLetter(Fixtures.record(Fixtures.envelope(id), partition, offset))

  // --- bounding -----------------------------------------------------------------------------------------------

  test("a limit over the ceiling is refused, not silently clamped"):
    // Clamping is the failure this test exists to prevent: the operator believes 500 events are back in the pipeline
    // and finds out from a gap in a dashboard that 300 are not.
    val refused = ReplayRequest.parse(500, "", "", dryRun = false, MaxRecords)
    assert(refused.isLeft)
    assert(refused.left.exists(_.contains("200")), refused)

  test("a negative limit is refused"):
    assert(ReplayRequest.parse(-1, "", "", dryRun = true, MaxRecords).isLeft)

  test("an unspecified limit resolves against the ceiling rather than against a literal"):
    // A Cask annotation default has to be a compile-time constant, so without this a deployment with a ceiling below
    // the default would answer 400 to every bare request — refused for exceeding a limit nobody asked to exceed.
    assertEquals(
      ReplayRequest.parse(ReplayRequest.UnspecifiedLimit, "", "", dryRun = true, MaxRecords).map(_.scope),
      Right(ReplayScope.Recent(ReplayRequest.DefaultLimit, None))
    )
    assertEquals(
      ReplayRequest.parse(ReplayRequest.UnspecifiedLimit, "", "", dryRun = true, maxRecords = 5).map(_.scope),
      Right(ReplayScope.Recent(5, None))
    )

  test("naming records is its own bound, and the limit is ignored"):
    val parsed = ReplayRequest.parse(1, "", "a/0/1, a/0/2 ,a/0/1", dryRun = true, MaxRecords)
    assertEquals(parsed.map(_.scope), Right(ReplayScope.Named(Vector("a/0/1", "a/0/2"))))
    assertEquals(parsed.map(_.fetchLimit(MaxRecords)), Right(MaxRecords))

  test("naming more records than the ceiling is refused"):
    val refs = (1 to 5).map(i => s"t/0/$i").mkString(",")
    assert(ReplayRequest.parse(10, "", refs, dryRun = true, maxRecords = 3).isLeft)

  test("a blank reason is no filter at all, not a filter for the empty string"):
    assertEquals(
      ReplayRequest.parse(5, "   ", "", dryRun = true, MaxRecords).map(_.scope),
      Right(ReplayScope.Recent(5, None))
    )

  // --- selection ----------------------------------------------------------------------------------------------

  test("the newest N are selected, but they are published oldest first"):
    // Selection is newest-first because "the last N" is what an operator means; publication is oldest-first so a
    // device's events reach the topic in the order they originally did rather than backwards.
    val records = Vector(
      Fixtures.dlqRecord(letter("c", offset = 3L), offset = 3L, timestamp = 300L),
      Fixtures.dlqRecord(letter("b", offset = 2L), offset = 2L, timestamp = 200L),
      Fixtures.dlqRecord(letter("a", offset = 1L), offset = 1L, timestamp = 100L)
    )
    val request = ReplayRequest(ReplayScope.Recent(2, None), dryRun = true)
    val plan = DeadLetterReplay.plan(records, request, Fixtures.Topic, MaxAttempts)
    assertEquals(plan.replays.map(_.deadLetter.origin.offset), Vector(2L, 3L))

  test("a reason filter keeps only that category, and drops records that have no category at all"):
    val malformed = Fixtures.dlqRecord(
      Fixtures.deadLetter(Fixtures.record(Fixtures.envelope("m"), offset = 1L), DecodeFailure.MalformedBinary("x")),
      offset = 1L
    )
    val unconvertible = Fixtures.dlqRecord(
      Fixtures.deadLetter(Fixtures.record(Fixtures.envelope("u"), offset = 2L), DecodeFailure.Unconvertible("y")),
      offset = 2L
    )
    val request = ReplayRequest(ReplayScope.Recent(10, Some("unconvertible")), dryRun = true)
    val records = Vector(malformed, unconvertible, Fixtures.unreadableDlqRecord(offset = 3L))
    val plan = DeadLetterReplay.plan(records, request, Fixtures.Topic, MaxAttempts)
    assertEquals(plan.decisions.size, 1)
    assertEquals(plan.replays.map(_.deadLetter.detail), Vector("y"))

  test("a named ref that is not in the fetch window refuses the whole operation"):
    val present = Fixtures.dlqRecord(letter("a", offset = 1L), offset = 1L)
    val request = ReplayRequest(ReplayScope.Named(Vector(present.ref, "missing/0/9")), dryRun = false)
    val plan = DeadLetterReplay.plan(Vector(present), request, Fixtures.Topic, MaxAttempts)
    assertEquals(plan.missing, Vector("missing/0/9"))
    assert(plan.refusal.isDefined, "one absent ref must refuse the whole named set")

  test("a named ref that cannot be replayed also refuses the whole operation"):
    val unreadable = Fixtures.unreadableDlqRecord(offset = 1L)
    val request = ReplayRequest(ReplayScope.Named(Vector(unreadable.ref)), dryRun = false)
    val plan = DeadLetterReplay.plan(Vector(unreadable), request, Fixtures.Topic, MaxAttempts)
    assert(plan.refusal.exists(_.contains("undecodable")), plan.refusal)

  test("a recent selection never refuses — a skipped record is an answer, not a failure"):
    val records = Vector(Fixtures.unreadableDlqRecord(offset = 1L), Fixtures.dlqRecord(letter("a"), offset = 2L))
    val request = ReplayRequest(ReplayScope.Recent(10, None), dryRun = false)
    val plan = DeadLetterReplay.plan(records, request, Fixtures.Topic, MaxAttempts)
    assertEquals(plan.refusal, None)
    assertEquals(plan.skips.map(_.reason), Vector(ReplaySkip.Undecodable))
    assertEquals(plan.replays.size, 1)

  // --- classification -----------------------------------------------------------------------------------------

  test("a dead letter from another topic is never republished"):
    // Without this the DLQ is a way to make cobalt produce to any topic in the cluster that a record can name.
    val foreign = Fixtures.deadLetter(
      ConsumerRecord(
        "someone.elses.topic",
        0,
        7L,
        0L,
        TimestampType.CREATE_TIME,
        -1,
        -1,
        "k",
        Array[Byte](1),
        Fixtures.record(Fixtures.envelope("a")).headers,
        java.util.Optional.empty[java.lang.Integer]
      )
    )
    val decision = DeadLetterReplay.classify(Fixtures.dlqRecord(foreign), Fixtures.Topic, MaxAttempts)
    assertEquals(decision, ReplayDecision.Skip(Fixtures.dlqRecord(foreign), ReplaySkip.ForeignTopic))

  test("a record that has never been replayed becomes generation one"):
    DeadLetterReplay.classify(Fixtures.dlqRecord(letter("a")), Fixtures.Topic, MaxAttempts) match
      case ReplayDecision.Replay(_, _, attempt) => assertEquals(attempt, 1)
      case other                                => fail(s"expected a replay, got $other")

  test("the replay budget is what bounds the poison loop"):
    val exhausted = letter("a").copy(headers = Map(ReplayHeaders.Attempt -> MaxAttempts.toString))
    val decision = DeadLetterReplay.classify(Fixtures.dlqRecord(exhausted), Fixtures.Topic, MaxAttempts)
    assertEquals(decision.asInstanceOf[ReplayDecision.Skip].reason, ReplaySkip.BudgetExhausted)

    val oneLeft = letter("a").copy(headers = Map(ReplayHeaders.Attempt -> (MaxAttempts - 1).toString))
    DeadLetterReplay.classify(Fixtures.dlqRecord(oneLeft), Fixtures.Topic, MaxAttempts) match
      case ReplayDecision.Replay(_, _, attempt) => assertEquals(attempt, MaxAttempts)
      case other                                => fail(s"the last generation must still be replayable, got $other")

  test("an unreadable attempt header counts as exhausted, never as zero"):
    // Nothing in this build writes anything else there, so an unparseable value means "I cannot tell how many times
    // this has gone round", and the safe reading of that is "do not send it round again".
    Vector("banana", "-1", "").foreach: tampered =>
      val letters = letter("a").copy(headers = Map(ReplayHeaders.Attempt -> tampered))
      val decision = DeadLetterReplay.classify(Fixtures.dlqRecord(letters), Fixtures.Topic, MaxAttempts)
      assertEquals(
        decision.asInstanceOf[ReplayDecision.Skip].reason,
        ReplaySkip.BudgetExhausted,
        s"'$tampered' must not be read as a fresh record"
      )

  test("attemptsOf reads an absent header as never replayed"):
    assertEquals(ReplayHeaders.attemptsOf(Map.empty), Right(0))
    assertEquals(ReplayHeaders.attemptsOf(Map(ReplayHeaders.Attempt -> " 2 ")), Right(2))
    assert(ReplayHeaders.attemptsOf(Map(ReplayHeaders.Attempt -> "2.5")).isLeft)

  // --- reconstruction -----------------------------------------------------------------------------------------

  test("a rebuilt record is the record that died, byte for byte"):
    // The property the whole feature rests on: the CloudEvents id and source survive unchanged, which is the only
    // reason ON CONFLICT DO NOTHING makes replaying an event that DID land a no-op.
    val original = Fixtures.record(Fixtures.envelope("evt-1"), partition = 4, offset = 99L)
    val letters = Fixtures.deadLetter(original, DecodeFailure.Unconvertible("pretend the database refused it"))
    val rebuilt = DeadLetterReplay.producerRecord(ReplayDecision.Replay(Fixtures.dlqRecord(letters), letters, 1))

    assertEquals(rebuilt.topic, Fixtures.Topic)
    assertEquals(rebuilt.key, original.key)
    assert(java.util.Arrays.equals(rebuilt.value, original.value), "the payload bytes must be copied, never re-encoded")
    assert(rebuilt.partition == null, "the broker assigns the partition; pinning it breaks topic expansion")
    assert(rebuilt.timestamp == null, "the broker assigns the timestamp; the original one would hide the replay")

    val originalCe = original.headers.iterator.asScala.map(h => h.key -> String(h.value, UTF_8)).toMap
    val rebuiltCe = rebuilt.headers.iterator.asScala
      .map(h => h.key -> String(h.value, UTF_8))
      .filterNot((key, _) => key.startsWith("x-worxbend-replay"))
      .toMap
    assertEquals(rebuiltCe, originalCe, "every original header must come back unchanged")

  test("a rebuilt record decodes to exactly what the original decoded to"):
    // Asserted as "the two decodes agree" rather than "the decode equals the envelope I built", so the property under
    // test is the replay's fidelity and not the codec's — the codec has its own round-trip property in eventing.
    val envelope = Fixtures.envelope("evt-round-trip")
    val original = Fixtures.record(envelope)
    val letters = Fixtures.deadLetter(original, DecodeFailure.Unconvertible("simulated write failure"))
    val rebuilt = DeadLetterReplay.producerRecord(ReplayDecision.Replay(Fixtures.dlqRecord(letters), letters, 1))
    val consumed = Fixtures.consumerRecord(rebuilt.key, rebuilt.value, rebuilt.headers, 0, 0L)
    assertEquals(KafkaCodecs.decode(consumed), KafkaCodecs.decode(original))
    assertEquals(KafkaCodecs.decode(consumed).map(_.id), Right(envelope.id))

  test("the replay markers are transport headers, never CloudEvents attributes"):
    // A ce_-prefixed header IS a context attribute under the Kafka binding, so spelling these ce_replayattempt would
    // change the event: a different extension set, a different raw jsonb, a different row.
    val letters = letter("a")
    val rebuilt = DeadLetterReplay.producerRecord(ReplayDecision.Replay(Fixtures.dlqRecord(letters), letters, 2))
    val added = rebuilt.headers.iterator.asScala.map(_.key).filter(_.startsWith("x-worxbend-replay")).toSet
    assertEquals(added, Set(ReplayHeaders.Attempt, ReplayHeaders.Of))
    added.foreach(name => assert(!name.startsWith(CloudEventHeaders.Prefix), s"$name would become an extension"))
    assertEquals(
      CloudEventHeaders.get(rebuilt.headers, ReplayHeaders.Attempt),
      Some("2"),
      "the generation must be written for the next dead letter to read"
    )
    assertEquals(CloudEventHeaders.get(rebuilt.headers, ReplayHeaders.Of), Some(letters.origin.dlqKey))

  test("a replay marker replaces its predecessor rather than accumulating"):
    val second = letter("a").copy(headers = Map(ReplayHeaders.Attempt -> "1", ReplayHeaders.Of -> "old/0/0"))
    val rebuilt = DeadLetterReplay.producerRecord(ReplayDecision.Replay(Fixtures.dlqRecord(second), second, 2))
    assertEquals(rebuilt.headers.headers(ReplayHeaders.Attempt).asScala.size, 1)
    assertEquals(CloudEventHeaders.get(rebuilt.headers, ReplayHeaders.Attempt), Some("2"))

  test("an event with no data replays as a null value, not as an empty one"):
    // Option[Binary] is None because the record value was null, which is the binding's "no data". An empty array here
    // would turn "no data" into "zero-byte data", which kernel's Payload deliberately distinguishes.
    val letters = letter("a").copy(payload = None)
    val rebuilt = DeadLetterReplay.producerRecord(ReplayDecision.Replay(Fixtures.dlqRecord(letters), letters, 1))
    assertEquals(rebuilt.value, null)

  test("a DLQ record is read back into the dead letter that was written"):
    val letters = letter("a", partition = 2, offset = 11L)
    val produced = Fixtures.force(KafkaCodecs.deadLetterRecord(letters, "dlq"))
    val consumed = ConsumerRecord(
      "dlq",
      0,
      0L,
      0L,
      TimestampType.CREATE_TIME,
      -1,
      -1,
      produced.key,
      produced.value,
      produced.headers,
      java.util.Optional.empty[java.lang.Integer]
    )
    assertEquals(DeadLetterReplay.readDeadLetter(consumed.headers, Option(consumed.value)), Right(letters))

  test("reading a record that is not a dead letter is a value, never an exception"):
    val notADeadLetter = Fixtures.record(Fixtures.envelope("a"))
    assert(DeadLetterReplay.readDeadLetter(notADeadLetter.headers, Option(notADeadLetter.value)).isLeft)
    assert(DeadLetterReplay.readDeadLetter(org.apache.kafka.common.header.internals.RecordHeaders(), None).isLeft)

  // --- rendering ----------------------------------------------------------------------------------------------

  test("the payload preview is bounded and says so"):
    val big = Binary.copyOf(Array.fill(DeadLetterReplay.PreviewBytes * 4)('x'.toByte))
    val json = DeadLetterReplay.preview(letter("a").copy(payload = Some(big))).hcursor
    assertEquals(json.get[Int]("bytes").toOption, Some(DeadLetterReplay.PreviewBytes * 4))
    assertEquals(json.get[Boolean]("truncated").toOption, Some(true))
    assertEquals(json.get[String]("text").toOption.map(_.length), Some(DeadLetterReplay.PreviewBytes))

  test("the CloudEvents identity comes from the headers of a binary record"):
    val identity = DeadLetterReplay.identity(letter("evt-id"))
    assertEquals(identity.get("id"), Some("evt-id"))
    assertEquals(identity.get("type"), Some("com.worxbend.test.telemetry"))
    assertEquals(identity.get("source"), Some("urn:worxbend:test:gateway"))

  test("the CloudEvents identity falls back to the payload of a structured record"):
    val envelope = Fixtures.envelope("evt-structured")
    val produced = Fixtures.force(KafkaCodecs.producerRecord(Fixtures.Topic, envelope, ContentMode.Structured))
    val consumed = Fixtures.consumerRecord(produced.key, produced.value, produced.headers, 0, 0L)
    val identity = DeadLetterReplay.identity(Fixtures.deadLetter(consumed, DecodeFailure.Unconvertible("x")))
    assertEquals(identity.get("id"), Some("evt-structured"))

  test("an unreadable DLQ record is listed with its problem rather than dropped"):
    val json = DeadLetterReplay.render(Fixtures.unreadableDlqRecord(offset = 5L)).hcursor
    assertEquals(json.get[Boolean]("readable").toOption, Some(false))
    assertEquals(json.get[String]("problem").toOption, Some("not a dead letter"))
    assertEquals(json.get[Long]("dlqOffset").toOption, Some(5L))

  test("a listing entry carries the reason, the origin and the replay generation"):
    val letters = letter("evt-render", partition = 3, offset = 42L)
    val json = DeadLetterReplay.render(Fixtures.dlqRecord(letters, partition = 1, offset = 7L)).hcursor
    assertEquals(json.get[String]("ref").toOption, Some(s"${Fixtures.Topic}/3/42"))
    assertEquals(json.get[String]("reason").toOption, Some("malformed-binary"))
    assertEquals(json.downField("origin").get[Long]("offset").toOption, Some(42L))
    assertEquals(json.downField("event").get[String]("id").toOption, Some("evt-render"))
    assertEquals(json.get[Int]("replayAttempts").toOption, Some(0))
