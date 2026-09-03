package mtest.kafka

import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.kafka.*
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite

class DataTypesTest extends AnyFunSuite {

  // --- OffsetRange ---

  test("1.OffsetRange - from < until produces Some") {
    val r = OffsetRange(Offset(5), Offset(10))
    assert(r.isDefined)
    assert(r.get.from == 5)
    assert(r.get.until == 10)
    assert(r.get.distance == 5)
    assert(r.get.to == 9)
  }

  test("2.OffsetRange - from == until produces None") {
    assert(OffsetRange(Offset(5), Offset(5)).isEmpty)
  }

  test("3.OffsetRange - from > until produces None") {
    assert(OffsetRange(Offset(10), Offset(5)).isEmpty)
  }

  test("4.OffsetRange - zero-based range") {
    val r = OffsetRange(Offset(0), Offset(1))
    assert(r.isDefined)
    assert(r.get.distance == 1)
    assert(r.get.to == 0)
  }

  // --- PartitionRange ---

  test("5.PartitionRange - toString format") {
    val tp = new TopicPartition("my-topic", 3)
    val or = OffsetRange(Offset(100), Offset(200)).get
    val pr = PartitionRange(tp, or)
    assert(pr.toString == "my-topic-3-100-199")
  }

  test("6.PartitionRange - Show instance matches toString") {
    val tp = new TopicPartition("test", 0)
    val or = OffsetRange(Offset(0), Offset(50)).get
    val pr = PartitionRange(tp, or)
    assert(pr.show == pr.toString)
  }

  // --- LagBehind ---

  test("7.LagBehind - computes lag correctly") {
    val lb = LagBehind(Offset(5), Offset(10))
    assert(lb.current == 5)
    assert(lb.end == 10)
    assert(lb.lag == 5)
  }

  test("8.LagBehind - zero lag when current == end") {
    val lb = LagBehind(Offset(10), Offset(10))
    assert(lb.lag == 0)
  }

  test("9.LagBehind - negative lag when current > end (edge case)") {
    val lb = LagBehind(Offset(15), Offset(10))
    assert(lb.lag == -5)
  }

  test("10.LagBehind - JSON round-trip") {
    val lb = LagBehind(Offset(100), Offset(250))
    val json = lb.asJson
    val decoded = decode[LagBehind](json.noSpaces)
    assert(decoded == Right(lb))
  }

  // --- Offset ---

  test("11.Offset - asLast decrements by 1") {
    assert(Offset(10).asLast.value == 9)
  }

  test("12.Offset - asLast at 0 stays at 0") {
    assert(Offset(0).asLast.value == 0)
  }

  test("13.Offset - subtraction") {
    assert(Offset(10) - Offset(3) == 7)
  }

  test("14.Offset - JSON round-trip") {
    val o = Offset(42)
    val decoded = decode[Offset](o.asJson.noSpaces)
    assert(decoded == Right(o))
  }

  // --- TopicName ---

  test("15.TopicName - JSON round-trip") {
    val tn = TopicName("my-topic")
    val decoded = decode[TopicName](tn.asJson.noSpaces)
    assert(decoded == Right(tn))
    assert(tn.show == "my-topic")
  }

  // --- GroupId ---

  test("17.GroupId - JSON round-trip") {
    val gid = GroupId("consumer-group-1")
    val decoded = decode[GroupId](gid.asJson.noSpaces)
    assert(decoded == Right(gid))
    assert(gid.show == "consumer-group-1")
  }

  // --- Partition ---

  test("19.Partition - subtraction") {
    assert(Partition(5) - Partition(2) == 3)
  }

  test("20.Partition - JSON round-trip") {
    val p = Partition(7)
    val decoded = decode[Partition](p.asJson.noSpaces)
    assert(decoded == Right(p))
  }
}
