package com.github.chenharryhua.nanjin.kafka

import cats.derived.auto.show._
import cats.implicits._
import org.scalatest.FunSuite

class ConsumeMessageAkkaTest extends FunSuite with ShowKafkaMessage with AkkaMessageBitraverse {
  val vessel = TopicDef[Key, aisClassAPositionReport]("sea_vessel_position_reports")

  test("akka stream should be able to consume data") {
    val chn = ctx.topic(vessel).akkaChannel
    chn.consumeValidMessages
      .map(_.show)
      .map(println)
      .take(3)
      .runWith(chn.ignoreSink)(chn.materializer)
  }
}
