package com.github.chenharryhua.nanjin.kafka

import org.scalatest.FunSuite
import cats.implicits._
import TopicDef._
import akka.stream.scaladsl.Sink
import cats.derived.auto.show._

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
