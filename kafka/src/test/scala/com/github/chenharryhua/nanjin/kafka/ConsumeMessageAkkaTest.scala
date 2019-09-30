package com.github.chenharryhua.nanjin.kafka

import java.time.LocalDateTime

import cats.derived.auto.show._
import cats.implicits._
import com.github.chenharryhua.nanjin.codec._
import org.scalatest.funsuite.AnyFunSuite

class ConsumeMessageAkkaTest extends AnyFunSuite {

  val vessel: TopicDef[Key, aisClassAPositionReport] =
    TopicDef[Key, aisClassAPositionReport]("sea_vessel_position_reports")

  test("akka stream should be able to consume data") {
    val run = ctx.topic(vessel).akkaResource.use { chn =>
      chn
        .updateConsumerSettings(_.withClientId("c-id"))
        .updateCommitterSettings(_.withParallelism(10))
        .consume
        .map(chn.messageDecoder.decodeValue)
        .map(_.show)
        .map(println)
        .take(3)
        .runWith(chn.ignoreSink)(chn.materializer)
    }
    run.unsafeRunSync()
  }

  test("assignment") {
    val datetime = LocalDateTime.now
    val ret = for {
      start <- vessel.in(ctx).consumer.beginningOffsets
      offsets = start.flatten[KafkaOffset].value.mapValues(_.value)
      _ <- vessel.in(ctx).akkaResource.use { chn =>
        chn
          .assign(offsets)
          .map(chn.recordDecoder.decode)
          .map(_.show)
          .take(1)
          .runWith(chn.ignoreSink)(chn.materializer)
      }
    } yield ()
    ret.unsafeRunSync
  }
}
