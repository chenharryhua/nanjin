package com.github.chenharryhua.nanjin.kafka

import java.time.LocalDateTime

import cats.implicits._
import org.scalatest.FunSuite

class ConsumeMessageAkkaTest extends FunSuite with ShowKafkaMessage {

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

  test("assignment") {
    val datetime = LocalDateTime.now
    val topic    = vessel.in(ctx)
    val ret = for {
      start <- topic.consumer.beginningOffsets
      offsets = start.flatten[Long].value
      _ <- vessel.in(ctx).akkaResource.use { chn =>
        chn
          .assign(offsets)
          .map(topic.decode)
          .map(_.show)
          .take(1)
          .runWith(chn.ignoreSink)(chn.materializer)
      }
    } yield ()
    ret.unsafeRunSync
  }
}
