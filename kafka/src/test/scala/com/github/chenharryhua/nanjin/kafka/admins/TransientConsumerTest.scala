package com.github.chenharryhua.nanjin.kafka.admins

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.admins.SnapshotConsumer
import com.github.chenharryhua.nanjin.kafka.buildConsumer.*
import com.github.chenharryhua.nanjin.kafka.{buildConsumer, Offset, Partition, TopicPartitionMap}
import fs2.kafka.consumer.MkConsumer
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndTimestamp}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
import munit.CatsEffectSuite

import java.time.{Instant, LocalDate}
import com.github.chenharryhua.nanjin.kafka.PureConsumerSettings

class TransientConsumerTest extends CatsEffectSuite {
  private val pcs = PureConsumerSettings
  test("1.offsetRangeFor - 1") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 10)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, Map.empty)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    consumer.flatMap(_.offsetRangeFor(DateTimeRange(sydneyTime))).map { res =>
      assert(res.nonEmpty)
      assert(res.treeMap.size == 3)
      assert(res.treeMap.forall(_._2.forall(_.distance == 10)))
    }
  }

  test("2.offsetRangeFor - 2") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] = Map(
      tp0 -> new OffsetAndTimestamp(5, 0),
      tp1 -> new OffsetAndTimestamp(5, 0),
      tp2 -> new OffsetAndTimestamp(5, 0))
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    consumer.flatMap(_.offsetRangeFor(DateTimeRange(sydneyTime))).map { res =>
      assert(res.treeMap.size == 3)
      assert(res.treeMap.forall(_._2.forall(_.distance == 10)))
    }
  }

  test("3.offsetRangeFor - 3") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] =
      Map(
        tp0 -> new OffsetAndTimestamp(5, 0),
        tp1 -> new OffsetAndTimestamp(5, 0),
        tp2 -> new OffsetAndTimestamp(5, 0))
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    consumer
      .flatMap(_.offsetRangeFor(DateTimeRange(sydneyTime).withEndTime(LocalDate.now())))
      .map { res =>
        assert(res.treeMap.size == 3)
        assert(res.treeMap.forall(_._2.exists(_.distance == 5)))
      }
  }

  test("4.offsetRangeFor - 4") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] = Map(tp0 -> null, tp1 -> null, tp2 -> null)
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    consumer
      .flatMap(_.offsetRangeFor(DateTimeRange(sydneyTime).withEndTime(LocalDate.now())))
      .map { res =>
        assert(res.nonEmpty)
        assert(res.treeMap.size == 3)
        assert(res.treeMap.forall(_._2.exists(_.distance == 10)))
      }
  }

  test("5.offsetRangeFor - 5") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> null, tp1 -> null, tp2 -> null)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] =
      Map(
        tp0 -> new OffsetAndTimestamp(5, 0),
        tp1 -> new OffsetAndTimestamp(5, 0),
        tp2 -> new OffsetAndTimestamp(5, 0))
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    consumer
      .flatMap(_.offsetRangeFor(DateTimeRange(sydneyTime).withEndTime(LocalDate.now())))
      .map { res =>
        assert(res.nonEmpty)
        assert(res.treeMap.size == 3)
        assert(res.treeMap.forall(_._2.exists(_.distance == 5)))
      }
  }

  test("6.offset for time") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] =
      Map(
        tp0 -> new OffsetAndTimestamp(5, 0),
        tp1 -> new OffsetAndTimestamp(5, 0),
        tp2 -> new OffsetAndTimestamp(5, 0))
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    val expected = TopicPartitionMap(forTime.map { case (tp, of) => tp -> Option(Offset(of)) })

    consumer.flatMap(_.offsetsForTimes(Instant.ofEpochSecond(1))).map { res =>
      assert(res.treeMap.size == 3)
      assert(res == expected)
    }
  }

  test("7.coverage") {
    val begin: Map[TopicPartition, java.lang.Long] = Map.empty
    val end: Map[TopicPartition, java.lang.Long] = Map.empty
    val forTime: Map[TopicPartition, OffsetAndTimestamp] = Map.empty
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    consumer.flatMap(_.commitSync(Map.empty))
  }

  test("8.partitionsFor lists every partition of the topic") {
    val mkConsumer: MkConsumer[IO] = buildConsumer(Map.empty, Map.empty, Map.empty)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    consumer.flatMap(_.partitionsFor).map { res =>
      assert(res.toSet == Set(tp0, tp1, tp2))
    }
  }

  test("9.beginningOffsets and endOffsets map raw offsets to Option[Offset]") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 3, tp2 -> 7)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, Map.empty)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))

    for {
      beg <- consumer.flatMap(_.beginningOffsets)
      fin <- consumer.flatMap(_.endOffsets)
    } yield {
      assert(beg.get(tp0).contains(Some(Offset(0))))
      assert(beg.get(tp1).contains(Some(Offset(3))))
      assert(beg.get(tp2).contains(Some(Offset(7))))

      assert(fin.treeMap.size == 3)
      assert(fin.treeMap.forall(_._2.contains(Offset(10))))
    }
  }

  test("10.a null end offset surfaces as Some(None)") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    // tp2 present but null -> the Kafka client reports the offset as unavailable -> Some(None)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> null)
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, Map.empty)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    consumer.flatMap(_.endOffsets).map { res =>
      assert(res.get(tp0).contains(Some(Offset(10))))
      assert(res.get(tp2).contains(None))
    }
  }

  test("11.offsetRangeFor(start, end) uses offsetsForTimes on both ends") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    // both start and end resolve to the same offsetsForTimes result; distance is 0
    val forTime: Map[TopicPartition, OffsetAndTimestamp] =
      Map(
        tp0 -> new OffsetAndTimestamp(5, 0),
        tp1 -> new OffsetAndTimestamp(5, 0),
        tp2 -> new OffsetAndTimestamp(5, 0))
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    consumer
      .flatMap(_.offsetRangeFor(Instant.ofEpochSecond(1), Instant.ofEpochSecond(2)))
      .map { res =>
        assert(res.treeMap.size == 3)
        // from == until (both 5) -> OffsetRange.apply returns None
        assert(res.treeMap.forall(_._2.isEmpty))
      }
  }

  test("12.offsetRangeForAll spans beginning to end") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 2, tp2 -> 4)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, Map.empty)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    consumer.flatMap(_.offsetRangeForAll).map { res =>
      assert(res.get(tp0).flatten.map(_.distance).contains(10))
      assert(res.get(tp1).flatten.map(_.distance).contains(8))
      assert(res.get(tp2).flatten.map(_.distance).contains(6))
    }
  }

  test("13.offsetRangeSince spans offsetsForTimes to end") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] =
      Map(
        tp0 -> new OffsetAndTimestamp(5, 0),
        tp1 -> new OffsetAndTimestamp(5, 0),
        tp2 -> new OffsetAndTimestamp(5, 0))
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    consumer.flatMap(_.offsetRangeSince(Instant.ofEpochSecond(1))).map { res =>
      assert(res.treeMap.size == 3)
      assert(res.treeMap.forall(_._2.exists(_.distance == 5)))
    }
  }

  test("14.resetOffsets variants run against the consumer without error") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] =
      Map(
        tp0 -> new OffsetAndTimestamp(5, 0),
        tp1 -> new OffsetAndTimestamp(5, 0),
        tp2 -> new OffsetAndTimestamp(5, 0))
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    consumer.flatMap { c =>
      c.resetOffsetsToBegin >> c.resetOffsetsToEnd >> c.resetOffsetsForTimes(Instant.ofEpochSecond(1))
    }
  }

  test("15.resetOffsetsToEnd tolerates a missing end offset") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    // tp2 missing -> None -> dropped by offsetsOf before committing
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10)
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, Map.empty)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    consumer.flatMap(_.resetOffsetsToEnd)
  }

  test("16.retrieveRecord assigns, seeks, and returns the record at the requested offset") {
    val expected = new ConsumerRecord[Array[Byte], Array[Byte]](topicName.value, 0, 5L, Array(1), Array(2))
    val mk_consumer =
      buildConsumer(Map.empty, Map.empty, Map.empty, records = Map((tp0, 5L) -> List(expected)))
    val consumer = mk_consumer(pcs).map(SnapshotConsumer[IO](topicName, _))

    consumer.flatMap(_.retrieveRecord(Partition(0), Offset(5L))).map { result =>
      assertEquals(result, Some(expected))
    }
  }

  test("17.retrieveFirstRecords skips unavailable offsets and sorts by partition") {
    val first_0 = new ConsumerRecord[Array[Byte], Array[Byte]](topicName.value, 0, 5L, Array(1), Array(2))
    val first_2 = new ConsumerRecord[Array[Byte], Array[Byte]](topicName.value, 2, 8L, Array(3), Array(4))
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 5L, tp1 -> null, tp2 -> 8L)
    val mk_consumer = buildConsumer(
      begin,
      Map.empty,
      Map.empty,
      records = Map((tp2, 8L) -> List(first_2), (tp0, 5L) -> List(first_0)))
    val consumer = mk_consumer(pcs).map(SnapshotConsumer[IO](topicName, _))

    consumer.flatMap(_.retrieveFirstRecords).map(result => assertEquals(result, List(first_0, first_2)))
  }

  test("18.retrieveLastRecords seeks to end minus one and skips empty partitions") {
    val last_0 = new ConsumerRecord[Array[Byte], Array[Byte]](topicName.value, 0, 5L, Array(1), Array(2))
    val last_2 = new ConsumerRecord[Array[Byte], Array[Byte]](topicName.value, 2, 9L, Array(3), Array(4))
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 6L, tp1 -> 0L, tp2 -> 10L)
    val mk_consumer = buildConsumer(
      Map.empty,
      end,
      Map.empty,
      records = Map((tp2, 9L) -> List(last_2), (tp0, 5L) -> List(last_0)))
    val consumer = mk_consumer(pcs).map(SnapshotConsumer[IO](topicName, _))

    consumer.flatMap(_.retrieveLastRecords).map(result => assertEquals(result, List(last_0, last_2)))
  }

  test("19.retrieveRecordsForTimes skips missing offsets") {
    val record_0 = new ConsumerRecord[Array[Byte], Array[Byte]](topicName.value, 0, 4L, Array(1), Array(2))
    val record_2 = new ConsumerRecord[Array[Byte], Array[Byte]](topicName.value, 2, 8L, Array(3), Array(4))
    val for_time: Map[TopicPartition, OffsetAndTimestamp] =
      Map(tp0 -> new OffsetAndTimestamp(4L, 100L), tp1 -> null, tp2 -> new OffsetAndTimestamp(8L, 100L))
    val mk_consumer = buildConsumer(
      Map.empty,
      Map.empty,
      for_time,
      records = Map((tp2, 8L) -> List(record_2), (tp0, 4L) -> List(record_0)))
    val consumer = mk_consumer(pcs).map(SnapshotConsumer[IO](topicName, _))

    consumer.flatMap(_.retrieveRecordsForTimes(Instant.ofEpochMilli(100L))).map { result =>
      assertEquals(result, List(record_0, record_2))
    }
  }

  test("20.retrieval helpers return empty lists when no partition has a readable offset") {
    val unavailable: Map[TopicPartition, java.lang.Long] = Map(tp0 -> null, tp1 -> null, tp2 -> null)
    val zero: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0L, tp2 -> 0L)
    val for_time: Map[TopicPartition, OffsetAndTimestamp] = Map(tp0 -> null, tp1 -> null, tp2 -> null)
    val mk_consumer = buildConsumer(unavailable, zero, for_time)
    val consumer = mk_consumer(pcs).map(SnapshotConsumer[IO](topicName, _))

    for {
      first <- consumer.flatMap(_.retrieveFirstRecords)
      last <- consumer.flatMap(_.retrieveLastRecords)
      timed <- consumer.flatMap(_.retrieveRecordsForTimes(Instant.EPOCH))
    } yield {
      assertEquals(first, Nil)
      assertEquals(last, Nil)
      assertEquals(timed, Nil)
    }
  }

  test("21.metrics returns the consumer metrics map") {
    val metric_name = new MetricName("records", "consumer", "records", java.util.Map.of())
    val metric = new Metric {
      override def metricName(): MetricName = metric_name
      override def metricValue(): Object = java.lang.Long.valueOf(42L)
    }
    val mk_consumer =
      buildConsumer(Map.empty, Map.empty, Map.empty, consumerMetrics = Map(metric_name -> metric))
    val consumer = mk_consumer(pcs).map(SnapshotConsumer[IO](topicName, _))

    consumer.flatMap(_.metrics).map { result =>
      assertEquals(result, Map(metric_name -> metric))
    }
  }
}
