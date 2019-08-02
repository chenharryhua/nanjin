package com.github.chenharryhua.nanjin.kafka

import akka.stream.scaladsl.Sink
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
  test("retrieve") {
    import topic.akkaStream.materializer
    val ret =
      topic.akkaStream.consumeValidMessages.map(_.show).take(3).map(println).runWith(Sink.ignore)
  }
}
