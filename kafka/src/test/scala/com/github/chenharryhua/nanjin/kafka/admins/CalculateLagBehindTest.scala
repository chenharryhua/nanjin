package com.github.chenharryhua.nanjin.kafka.admins

import com.github.chenharryhua.nanjin.kafka.{LagBehind, Offset, TopicPartitionMap}
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite

/** Lives in package `com.github.chenharryhua.nanjin.kafka.admins` so it can reach the package-private
  * `calculate` object. `calculate.admin_lagBehind` is the pure core of `AdminTopicGroup.lagBehind`: given the
  * per-partition end offsets and the consumer group's committed offsets, it produces the per-partition lag.
  * `AdminTopicGroup.lagBehind` only adds the effectful fetching and topic filtering around this call, so
  * pinning the computation here covers the behavior that matters without a live broker.
  */
class CalculateLagBehindTest extends AnyFunSuite {

  private val topic = "topic"
  private def tp(partition: Int): TopicPartition = new TopicPartition(topic, partition)

  private def ends(entries: (Int, Option[Long])*): TopicPartitionMap[Option[Offset]] =
    TopicPartitionMap(entries.map { case (p, o) => tp(p) -> o.map(Offset(_)) })

  private def curr(entries: (Int, Long)*): TopicPartitionMap[Offset] =
    TopicPartitionMap(entries.map { case (p, o) => tp(p) -> Offset(o) })

  test("1.lag is end minus current, with current and end carried through") {
    val result = calculate.admin_lagBehind(ends(0 -> Some(100L)), curr(0 -> 70L))
    val lag = result.get(topic, 0).flatten
    assert(lag.contains(LagBehind(Offset(70L), Offset(100L))))
    assert(lag.map(_.lag).contains(30L))
  }

  test("2.a fully caught-up partition has zero lag") {
    val result = calculate.admin_lagBehind(ends(0 -> Some(50L)), curr(0 -> 50L))
    val lag = result.get(topic, 0).flatten
    assert(lag.contains(LagBehind(Offset(50L), Offset(50L))))
    assert(lag.map(_.lag).contains(0L))
  }

  test("3.a partition whose end offset is unavailable yields None") {
    // end is None (e.g. the broker did not report an end offset): the entry is present but empty
    val result = calculate.admin_lagBehind(ends(0 -> None), curr(0 -> 10L))
    assert(result.get(topic, 0).contains(None))
  }

  test("4.leftCombine is driven by ends: a committed partition absent from ends is dropped") {
    // partition 1 has a committed offset but no end offset entry -> it does not appear in the result
    val result = calculate.admin_lagBehind(ends(0 -> Some(100L)), curr(0 -> 90L, 1 -> 5L))
    assert(result.get(topic, 0).flatten.contains(LagBehind(Offset(90L), Offset(100L))))
    assert(result.get(topic, 1).isEmpty)
  }

  test("5.a partition in ends with no committed offset yields None") {
    // partition 0 has an end offset but the group never committed to it -> None (leftCombine keeps the key)
    val result = calculate.admin_lagBehind(ends(0 -> Some(100L)), curr())
    assert(result.get(topic, 0).contains(None))
  }

  test("6.partitions are computed independently") {
    val result =
      calculate.admin_lagBehind(ends(0 -> Some(100L), 1 -> Some(200L)), curr(0 -> 100L, 1 -> 150L))
    assert(result.get(topic, 0).flatten.contains(LagBehind(Offset(100L), Offset(100L))))
    assert(result.get(topic, 1).flatten.contains(LagBehind(Offset(150L), Offset(200L))))
    assert(result.get(topic, 1).flatten.map(_.lag).contains(50L))
  }
}
