package com.github.chenharryhua.nanjin.kafka

import cats.implicits.catsSyntaxPartialOrder
import io.circe.generic.JsonCodec
import io.circe.syntax.EncoderOps
import io.circe.jawn.decode
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite

@JsonCodec
final case class Abc(a: Int, b: String, c: Long)

class TopicPartitionOrderTest extends AnyFunSuite {
  test("order") {
    val tp0 = new TopicPartition("a", 0)
    val tp1 = new TopicPartition("a", 1)
    val tp2 = new TopicPartition("b", 0)
    val tp3 = new TopicPartition("b", 1)

    assert(tp1 > tp0)
    assert(tp2 > tp1)
    assert(tp3 > tp1)
    assert(tp3 > tp2)
  }

  test("json") {
    val prim = TopicPartitionMap[Int](
      Map(
        new TopicPartition("a", 1) -> 1,
        new TopicPartition("a", 2) -> 2,
        new TopicPartition("a", 3) -> 3
      ))
    assert(decode[TopicPartitionMap[Int]](prim.asJson.spaces2).toOption.get == prim)

    val abc = TopicPartitionMap[Abc](
      Map(
        new TopicPartition("a", 1) -> Abc(1, "a", 1),
        new TopicPartition("a", 2) -> Abc(2, "b", 2),
        new TopicPartition("a", 3) -> Abc(3, "c", 3)
      ))
    assert(decode[TopicPartitionMap[Abc]](abc.asJson.spaces2).toOption.get == abc)
  }

  test("topic partition json") {
    val tp = new TopicPartition("a", 1)
    assert(decode[TopicPartition](tp.asJson.spaces2).toOption.get == tp)
  }

  test("partition") {
    val partition = Partition(1)
    assert(decode[Partition](partition.asJson.noSpaces).toOption.get == partition)
  }

  test("group id") {
    val gid = GroupId("abc")
    assert(decode[GroupId](gid.asJson.noSpaces).toOption.get == gid)
  }
}
