package com.github.chenharryhua.nanjin.kafka.connector

import cats.Foldable
import cats.data.NonEmptySet
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.foldable.given
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{EmptyTopicPartitionMap, TopicName, TopicPartitionMap, given}
import fs2.Stream
import fs2.kafka.CommittableConsumerRecord
import fs2.kafka.consumer.{KafkaAssignment, KafkaOffsets, KafkaTopics}
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetAndTimestamp}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import munit.CatsEffectSuite

import java.time.Instant
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.FiniteDuration

/** Offline coverage for the two concrete helpers on `ConsumerService`, `assignByTime` and `assignByMap`.
  *
  * Both drive a `KafkaAssignment & KafkaTopics & KafkaOffsets` consumer, so they are exercised here against
  * an in-memory recording stub (the same capability-trait approach `TopicUtilsTest` uses, lifted to `IO`).
  * The stub records every `assign`/`seek`/`seekToEnd` so the tests can assert the exact calls the helper
  * issues.
  *
  * The abstract stream methods (`subscribe`, `assign`, `manualCommitStream`, `circumscribedStream`) drive a
  * real consumer against a broker and are covered by the broker-backed connector suites; they are out of
  * scope here.
  */
class ConsumerServiceTest extends CatsEffectSuite {

  private val topic: String = "consumer-service-test"
  private val tn: TopicName = TopicName(topic)

  private def tp(partition: Int): TopicPartition = new TopicPartition(topic, partition)

  /** One recorded consumer interaction, in the order the helper issued it. */
  sealed private trait Call
  final private case class AssignTopic(topic: String) extends Call
  final private case class AssignPartitions(partitions: Set[TopicPartition]) extends Call
  final private case class Seek(partition: TopicPartition, offset: Long) extends Call
  final private case class SeekToEnd(partitions: Set[TopicPartition]) extends Call

  /** A minimal in-memory consumer implementing the three capability traits the helpers need. It reports
    * `partitions` from `partitionsFor`, answers `offsetsForTimes` from `forTimes`, and appends every
    * assignment/seek call to `calls` so a test can assert the resulting interaction. Methods the helpers
    * never call return empty results.
    */
  final private class RecordingConsumer(
    partitions: List[Int],
    forTimes: Map[TopicPartition, Option[OffsetAndTimestamp]],
    calls: Ref[IO, Vector[Call]]
  ) extends KafkaAssignment[IO] with KafkaTopics[IO] with KafkaOffsets[IO] {

    // KafkaAssignment
    override def assign(topic: String): IO[Unit] =
      calls.update(_ :+ AssignTopic(topic))
    override def assign(partitions: NonEmptySet[TopicPartition]): IO[Unit] =
      calls.update(_ :+ AssignPartitions(partitions.toSortedSet))
    override def assignment: IO[SortedSet[TopicPartition]] =
      IO.pure(SortedSet.empty[TopicPartition])
    override def assignmentStream: Stream[IO, SortedSet[TopicPartition]] =
      Stream.empty

    // KafkaTopics
    override def partitionsFor(t: String): IO[List[PartitionInfo]] =
      IO.pure(partitions.map(p => new PartitionInfo(topic, p, null, Array.empty, Array.empty)))
    override def partitionsFor(t: String, timeout: FiniteDuration): IO[List[PartitionInfo]] =
      partitionsFor(t)
    override def beginningOffsets(ps: Set[TopicPartition]): IO[Map[TopicPartition, Long]] =
      IO.pure(ps.map(_ -> 0L).toMap)
    override def beginningOffsets(
      ps: Set[TopicPartition],
      timeout: FiniteDuration): IO[Map[TopicPartition, Long]] = beginningOffsets(ps)
    override def endOffsets(ps: Set[TopicPartition]): IO[Map[TopicPartition, Long]] =
      IO.pure(ps.map(_ -> 0L).toMap)
    override def endOffsets(ps: Set[TopicPartition], timeout: FiniteDuration): IO[Map[TopicPartition, Long]] =
      endOffsets(ps)
    override def offsetsForTimes(
      ts: Map[TopicPartition, Long]): IO[Map[TopicPartition, Option[OffsetAndTimestamp]]] =
      IO.pure(ts.map { case (p, _) => p -> forTimes.getOrElse(p, None) })
    override def offsetsForTimes(
      ts: Map[TopicPartition, Long],
      timeout: FiniteDuration): IO[Map[TopicPartition, Option[OffsetAndTimestamp]]] =
      offsetsForTimes(ts)
    override def listTopics: IO[Map[String, List[PartitionInfo]]] = IO.pure(Map.empty)
    override def listTopics(timeout: FiniteDuration): IO[Map[String, List[PartitionInfo]]] =
      IO.pure(Map.empty)

    // KafkaOffsets
    override def seek(partition: TopicPartition, offset: Long): IO[Unit] =
      calls.update(_ :+ Seek(partition, offset))
    override def seekToEnd[G[_]](partitions: G[TopicPartition])(using Foldable[G]): IO[Unit] =
      calls.update(_ :+ SeekToEnd(partitions.toList.toSet))
    override def seekToBeginning[G[_]](partitions: G[TopicPartition])(using Foldable[G]): IO[Unit] =
      IO.unit
    override def committed(partitions: Set[TopicPartition]): IO[Map[TopicPartition, OffsetAndMetadata]] =
      IO.pure(Map.empty)
    override def committed(
      partitions: Set[TopicPartition],
      timeout: FiniteDuration): IO[Map[TopicPartition, OffsetAndMetadata]] = IO.pure(Map.empty)
    override def position(partition: TopicPartition): IO[Long] = IO.pure(0L)
    override def position(partition: TopicPartition, timeout: FiniteDuration): IO[Long] = IO.pure(0L)
  }

  /** Exposes the `protected` helpers on `ConsumerService`. The stream members are never invoked. */
  private object service extends ConsumerService[IO, Unit, Unit] {
    def runAssignByTime(
      kc: KafkaAssignment[IO] & KafkaTopics[IO] & KafkaOffsets[IO],
      time: Instant): IO[Unit] = assignByTime(kc, tn, time)
    def runAssignByMap(
      kc: KafkaAssignment[IO] & KafkaTopics[IO] & KafkaOffsets[IO],
      map: Map[Int, Long]): IO[Unit] = assignByMap(kc, tn, map)

    override def subscribe: Stream[IO, CommittableConsumerRecord[IO, Unit, Unit]] = ???
    override def partitionsMapStream
      : Stream[IO, TopicPartitionMap[Stream[IO, CommittableConsumerRecord[IO, Unit, Unit]]]] = ???
    override def assign: Stream[IO, CommittableConsumerRecord[IO, Unit, Unit]] = ???
    override def assign(
      partitionOffsets: Map[Int, Long]): Stream[IO, CommittableConsumerRecord[IO, Unit, Unit]] = ???
    override def assign(time: Instant): Stream[IO, CommittableConsumerRecord[IO, Unit, Unit]] = ???
    override def manualCommitStream: Stream[IO, ManualCommitStream[IO, Unit, Unit]] = ???
    override def circumscribedStream(
      dateTimeRange: DateTimeRange): Stream[IO, CircumscribedStream[IO, Unit, Unit]] = ???
    override def circumscribedStream(
      partitionOffsets: Map[Int, (Long, Long)]): Stream[IO, CircumscribedStream[IO, Unit, Unit]] = ???
  }

  private def record(
    partitions: List[Int],
    forTimes: Map[TopicPartition, Option[OffsetAndTimestamp]] = Map.empty)(
    run: (RecordingConsumer, Ref[IO, Vector[Call]]) => IO[Unit]): IO[Vector[Call]] =
    for {
      ref <- Ref.of[IO, Vector[Call]](Vector.empty)
      consumer = new RecordingConsumer(partitions, forTimes, ref)
      _ <- run(consumer, ref)
      calls <- ref.get
    } yield calls

  private def offsetAt(offset: Long): OffsetAndTimestamp = new OffsetAndTimestamp(offset, 0L)

  test("1.assignByMap assigns the partitions then seeks each to its offset") {
    record(partitions = List(0, 1, 2)) { (c, _) =>
      service.runAssignByMap(c, Map(0 -> 10L, 1 -> 20L, 2 -> 30L))
    }.map { calls =>
      assertEquals(calls.head, AssignPartitions(Set(tp(0), tp(1), tp(2))))
      // seeks follow the assignment, one per entry, in partition order
      assertEquals(calls.tail.toList, List(Seek(tp(0), 10L), Seek(tp(1), 20L), Seek(tp(2), 30L)))
    }
  }

  test("2.assignByMap raises EmptyTopicPartitionMap on an empty map, issuing no calls") {
    record(partitions = List(0, 1)) { (c, _) =>
      service.runAssignByMap(c, Map.empty).attempt.flatMap {
        case Left(EmptyTopicPartitionMap(name)) => IO(assertEquals(name, tn))
        case other                              => IO(fail(s"expected EmptyTopicPartitionMap, got $other"))
      }
    }.map(calls => assert(calls.isEmpty, s"expected no consumer calls, got $calls"))
  }

  test("3.assignByTime assigns the topic then seeks each partition to its offset-for-time") {
    val forTimes = Map(tp(0) -> Some(offsetAt(5L)), tp(1) -> Some(offsetAt(7L)))
    record(partitions = List(0, 1), forTimes = forTimes) { (c, _) =>
      service.runAssignByTime(c, Instant.ofEpochSecond(1))
    }.map { calls =>
      assertEquals(calls.head, AssignTopic(topic))
      assertEquals(calls.count(_.isInstanceOf[SeekToEnd]), 0)
      assert(calls.contains(Seek(tp(0), 5L)))
      assert(calls.contains(Seek(tp(1), 7L)))
    }
  }

  test("4.assignByTime seeks to end for a partition with no offset-for-time") {
    // tp1 has no offset at or after the time -> seekToEnd; tp0 has one -> seek
    val forTimes = Map(tp(0) -> Some(offsetAt(5L)), tp(1) -> None)
    record(partitions = List(0, 1), forTimes = forTimes) { (c, _) =>
      service.runAssignByTime(c, Instant.ofEpochSecond(1))
    }.map { calls =>
      assertEquals(calls.head, AssignTopic(topic))
      assert(calls.contains(Seek(tp(0), 5L)))
      assert(calls.contains(SeekToEnd(Set(tp(1)))))
      assert(!calls.contains(Seek(tp(1), 5L)))
    }
  }

  test("5.assignByTime on a topic with no partitions only assigns, no seeks") {
    record(partitions = Nil) { (c, _) =>
      service.runAssignByTime(c, Instant.ofEpochSecond(1))
    }.map { calls =>
      assertEquals(calls, Vector[Call](AssignTopic(topic)))
    }
  }
}
