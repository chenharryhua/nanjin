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
  test("akka stream should be able to consume data") {
    import topic.akkaStream.materializer
    val ret =
      topic.akkaStream.consumeValidMessages
        .map(_.show)
        .take(3)
        .runWith(Sink.fold(0)((s, _) => s + 1))
       // .map(n => assert(n == 3))
  }
}
