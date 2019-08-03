package com.github.chenharryhua.nanjin.kafka

import cats.derived.auto.show._
import cats.implicits._
import org.scalatest.FunSuite

class ConsumeMessageAkkaTest extends FunSuite with ShowKafkaMessage with AkkaMessageBitraverse {
  val vessel = TopicDef[Key, aisClassAPositionReport]("sea_vessel_position_reports")

  val topic = ctx.topic(vessel)
  ignore("schema") {
    val ret = topic.schemaRegistry.latestMeta.unsafeRunSync().show
    println(ret)
  }
  test("akka stream should be able to consume data") {
    import topic.akkaStream._
    val ret =
      consumeValidMessages.map(_.show).map(println).take(3).runWith(ignoreSink)
    ret.unsafeRunSync
  }
}
