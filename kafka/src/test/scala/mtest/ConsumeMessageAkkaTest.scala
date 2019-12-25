package mtest

import java.time.LocalDateTime
import com.github.chenharryhua.nanjin.codec._

import cats.derived.auto.show._
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka._
import com.github.chenharryhua.nanjin.codec.show._ 
import org.scalatest.funsuite.AnyFunSuite
import io.circe.generic.auto._ 

class ConsumeMessageAkkaTest extends AnyFunSuite {

  val vessel: TopicDef[Key, aisClassAPositionReport] =
    TopicDef[Key, aisClassAPositionReport]("sea_vessel_position_reports")
  val topic = ctx.topic(vessel)
  test("akka stream should be able to consume data") {

    val run = topic.akkaResource.use { chn =>
      chn
        .updateConsumerSettings(_.withClientId("c-id"))
        .updateCommitterSettings(_.withParallelism(10))
        .consume
        .map(m => topic.decoder(m).decodeValue)
        .map(_.show)
        .map(println)
        .take(3)
        .runWith(chn.ignoreSink)(ctx.materializer.value)
    }
    run.unsafeRunSync()
  }

  test("assignment") {
    val datetime = LocalDateTime.now
    val ret = for {
      start <- topic.consumer.beginningOffsets
      offsets = start.flatten[KafkaOffset].value.mapValues(_.value)
      _ <- vessel.in(ctx).akkaResource.use { chn =>
        chn
          .assign(offsets)
          .map(m => topic.decoder(m).decode)
          .map(_.show)
          .take(1)
          .runWith(chn.ignoreSink)(ctx.materializer.value)
      }
    } yield ()
    ret.unsafeRunSync
  }
}
