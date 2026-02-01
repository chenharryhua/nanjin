package mtest.kafka

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.kafka.{
  GroupId,
  LagBehind,
  ListOfTopicPartitions,
  Offset,
  OffsetRange,
  Partition,
  TopicPartitionMap
}
import io.circe.jawn.decode
import io.circe.syntax.*
import munit.CatsEffectSuite
import org.apache.kafka.common.TopicPartition

class KafkaTypesSuite extends CatsEffectSuite {

  test("GroupId codec") {
    val gid = GroupId("my-group")
    val json = gid.asJson.noSpaces
    assertEquals(json, "\"my-group\"")
    assertEquals(decode[GroupId](json), Right(gid))
  }

  test("Offset arithmetic and asLast") {
    val o1 = Offset(10)
    val o2 = Offset(3)
    assertEquals(o1 - o2, 7L)
    assertEquals(Offset(0).asLast.value, 0L)
    assertEquals(Offset(5).asLast.value, 4L)
  }

  test("Partition arithmetic") {
    val p1 = Partition(7)
    val p2 = Partition(2)
    assertEquals(p1 - p2, 5)
  }

  test("OffsetRange creation and distance") {
    val rOpt = OffsetRange(Offset(5), Offset(10))
    assert(rOpt.isDefined)
    val r = rOpt.get
    assertEquals(r.distance, 5L)
    assertEquals(r.to, 9L)

    val empty = OffsetRange(Offset(10), Offset(5))
    assert(empty.isEmpty)
  }

  test("OffsetRange PartialOrder") {
    val r1 = OffsetRange(Offset(2), Offset(5)).get
    val r2 = OffsetRange(Offset(1), Offset(6)).get
    val r3 = OffsetRange(Offset(2), Offset(5)).get
    val r4 = OffsetRange(Offset(0), Offset(3)).get

    assertEquals(OffsetRange.poOffsetRange.partialCompare(r1, r2), -1.0)
    assertEquals(OffsetRange.poOffsetRange.partialCompare(r1, r3), 0.0)
    assert(OffsetRange.poOffsetRange.partialCompare(r1, r4).isNaN)
  }

  test("LagBehind calculation") {
    val lag = LagBehind(Offset(3), Offset(10))
    assertEquals(lag.lag, 7L)
    assertEquals(lag.current, 3L)
    assertEquals(lag.end, 10L)
  }

  test("ListOfTopicPartitions utilities") {
    val tp1 = new TopicPartition("t1", 0)
    val tp2 = new TopicPartition("t2", 1)
    val ltp = ListOfTopicPartitions(List(tp1, tp2))
    val ts = NJTimestamp.now()
    val javaMap = ltp.javaTimed(ts)
    assertEquals(javaMap.size(), 2)
    val javaList = ltp.asJava
    assertEquals(javaList.size(), 2)
  }

  test("TopicPartitionMap basic operations") {
    val tp1 = new TopicPartition("t1", 0)
    val tp2 = new TopicPartition("t1", 1)
    val tpMap = TopicPartitionMap(Map(tp1 -> Offset(5), tp2 -> Offset(10)))

    assert(tpMap.nonEmpty)
    assertEquals(tpMap.get(tp1).map(_.value), Some(5L))
    assertEquals(tpMap.get("t1", 1).map(_.value), Some(10L))
    assertEquals(tpMap.topicPartitions.value.size, 2)
  }

  test("TopicPartitionMap mapValues and map") {
    val tp1 = new TopicPartition("t1", 0)
    val tp2 = new TopicPartition("t1", 1)
    val tpMap = TopicPartitionMap(Map(tp1 -> Offset(5), tp2 -> Offset(10)))
    val doubled = tpMap.mapValues(o => Offset(o.value * 2))
    assertEquals(doubled.get(tp1).map(_.value), Some(10L))
    val summed = tpMap.map((_, o) => o.value + 1)
    assertEquals(summed.get(tp2), Some(11L))
  }

  test("TopicPartitionMap intersectCombine and leftCombine") {
    val tp1 = new TopicPartition("t1", 0)
    val tp2 = new TopicPartition("t1", 1)
    val tp3 = new TopicPartition("t2", 0)

    val a = TopicPartitionMap(Map(tp1 -> Offset(5), tp2 -> Offset(10)))
    val b = TopicPartitionMap(Map(tp1 -> Offset(3), tp3 -> Offset(8)))

    val intersected = a.intersectCombine(b)((x, y) => x.value + y.value)
    assertEquals(intersected.value.keySet, Set(tp1))
    assertEquals(intersected.get(tp1), Some(8L))

    val left = a.leftCombine(b)((x, y) => Some(x.value - y.value))
    assertEquals(left.get(tp1), Some(Some(2L)))
    assertEquals(left.get(tp2), Some(None))
  }

  test("TopicPartitionMap flatten") {
    val tp1 = new TopicPartition("t1", 0)
    val tp2 = new TopicPartition("t1", 1)
    val mapOpt = TopicPartitionMap(Map(tp1 -> Some(5), tp2 -> None))
    val flat = mapOpt.flatten
    assertEquals(flat.value.keySet, Set(tp1))
    assertEquals(flat.get(tp1), Some(5))
  }

  test("Circe encoding/decoding TopicPartitionMap") {
    val tp1 = new TopicPartition("t1", 0)
    val tpMap = TopicPartitionMap(Map(tp1 -> Offset(42)))
    val json = tpMap.asJson.noSpaces
    val decoded = decode[TopicPartitionMap[Offset]](json)
    assertEquals(decoded, Right(tpMap))
  }
}
