package com.github.chenharryhua.nanjin.kafka

import cats.effect.IO
import org.scalatest.FunSuite

class IsoTest extends FunSuite {

  val topic: KafkaTopic[IO, Int, Payment] = TopicDef[Int, Payment]("payment").in(ctx)

  test("should handle null") {
    val k = topic.keyIso.reverseGet(10)
    val g = topic.keyIso.get(k)
    assert(g === 10)
    val p   = Payment("a", "b", 12, "c", "d", 12)
    val abp = topic.valueIso.reverseGet(p)
    assert(topic.valueIso.get(abp) === p)
    val n  = topic.valueIso.reverseGet(null)
    val nn = topic.valueIso.get(n)
    assert(n === null)
    assert(nn === null)
  }
}
