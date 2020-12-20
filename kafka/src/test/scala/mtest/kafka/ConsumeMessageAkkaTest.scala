package mtest.kafka

import java.time.LocalDateTime
import cats.derived.auto.show._
import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka.{KafkaOffset, TopicName, _}
import org.scalatest.funsuite.AnyFunSuite
import cats.effect.IO
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.sksamuel.avro4s.SchemaFor

class ConsumeMessageAkkaTest extends AnyFunSuite {

  val vessel: TopicDef[PKey, aisClassAPositionReport] =
    TopicDef[PKey, aisClassAPositionReport](
      TopicName("sea_vessel_position_reports"),
      AvroCodec[aisClassAPositionReport])
  val topic = ctx.topic(vessel)
  val chn   = topic.akkaChannel

  test("akka stream should be able to consume data") {
    val run = chn
      .withConsumerSettings(_.withClientId("c-id"))
      .withCommitterSettings(_.withParallelism(10))
      .source
      .map(m => topic.decoder(m).decodeValue)
      .map(_.toString)
      .map(println)
      .take(1)
      .runWith(akkaSinks.ignore[IO])

    run.unsafeRunSync()
  }

  test("assignment") {
    val datetime = LocalDateTime.now
    val ret = for {
      start <- topic.shortLiveConsumer.use(_.beginningOffsets)
      offsets = start.flatten[KafkaOffset].value.mapValues(_.value)
      _ <-
        chn
          .assign(offsets)
          .map(m => topic.decoder(m).decode)
          .map(_.toString)
          .take(1)
          .runWith(akkaSinks.ignore[IO])
    } yield ()
    ret.unsafeRunSync
  }
}
