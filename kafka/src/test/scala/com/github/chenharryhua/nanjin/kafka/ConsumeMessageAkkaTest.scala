package com.github.chenharryhua.nanjin.kafka

import cats.implicits._
import org.scalatest.FunSuite

class ConsumeMessageAkkaTest extends FunSuite with ShowKafkaMessage with AkkaMessageBitraverse {
  val vessel = TopicDef[Key, aisClassAPositionReport]("sea_vessel_position_reports")

  test("akka stream should be able to consume data") {
    val ret =
      ctx.topic(vessel).akkaResource.use { a =>
        a.consumeValidMessages
          .map(_.show)
          .map(println)
          .take(3)
          .runWith(a.ignoreSink)(a.materializer)
      }
    ret.unsafeRunSync
  }
}
