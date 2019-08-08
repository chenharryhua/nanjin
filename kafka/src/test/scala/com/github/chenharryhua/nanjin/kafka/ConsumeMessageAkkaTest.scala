package com.github.chenharryhua.nanjin.kafka

import cats.implicits._
import org.scalatest.FunSuite

class ConsumeMessageAkkaTest extends FunSuite with ShowKafkaMessage with AkkaMessageBitraverse {

  val vessel: TopicDef[Key, aisClassAPositionReport] =
    TopicDef[Key, aisClassAPositionReport]("sea_vessel_position_reports")

  test("akka stream should be able to consume data") {
    val run = ctx.topic(vessel).akkaResource.use { chn =>
      chn
        .updateConsumerSettings(s => s.withClientId("c-id"))
        .updateCommitterSettings(s => s.withParallelism(10))
        .consume
        .map(chn.decodeValue)
        .map(_.show)
        .map(println)
        .take(3)
        .runWith(chn.ignoreSink)(chn.materializer)
    }
    run.unsafeRunSync()
  }
}
