package com.github.chenharryhua.nanjin.kafka

import akka.stream.scaladsl.Sink
import cats.derived.auto.show._
import cats.implicits._
import org.scalatest.FunSuite

class ConsumeMessageAkkaTest extends FunSuite with ShowKafkaMessage with AkkaMessageBitraverse {
  val vessel = TopicDef[Key, aisClassAPositionReport]("sea_vessel_position_reports")

  test("akka stream should be able to consume data") {
    val chn = ctx
      .topic(vessel)
      .akkaChannel
      .updateConsumerSettings(s => s.withClientId("c-id"))
      .updateCommitterSettings(s => s.withParallelism(10))
      .consumeValidMessages
      .map(_.show)
      .map(println)
      .take(3)
      .runWith(Sink.ignore)(ctx.materializer)
  }
}
