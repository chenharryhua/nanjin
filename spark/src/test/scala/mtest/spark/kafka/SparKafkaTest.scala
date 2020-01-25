package mtest.spark.kafka

import java.time.LocalDate

import cats.effect.IO
import cats.implicits._
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.kafka._
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.datetime.iso._
import frameless.cats.implicits._
import cats.derived.auto.show._
import io.circe.syntax._
import io.circe.generic.auto._
import com.github.chenharryhua.nanjin.kafka.TopicDef
import java.time.Instant

import com.github.chenharryhua.nanjin.kafka.codec.ManualAvroSchema
import com.landoop.transportation.nyc.trip.yellow.trip_record

class SparKafkaTest extends AnyFunSuite {
  val embed = EmbeddedForTaskSerializable(0, "embeded")
  val data  = ForTaskSerializable(0, "a", LocalDate.now, Instant.now, embed)
  val topic = ctx.topic[Int, ForTaskSerializable]("serializable.test")

  (topic.admin.idefinitelyWantToDeleteTheTopic >> topic.schemaRegistry.register >>
    topic.send(List(0 -> data, 1 -> data))).unsafeRunSync()

  test("read topic from kafka") {
    val rst = topic.description.sparKafka
      .datasetFromKafka[IO]
      .flatMap(_.collect[IO]().map(_.flatMap(_.value).toList === List(data, data)))
      .unsafeRunSync
    assert(rst)
  }

  test("save topic to disk") {
    topic.description.sparKafka.withParamUpdate(_.withOverwrite).save[IO].unsafeRunSync
  }

  test("read topic from disk") {
    val rst = topic.description.sparKafka.load.collect[IO]().unsafeRunSync
    assert(rst.flatMap(_.value).toList === List(data, data))
  }

  test("replay") {
    topic.description.sparKafka.replay[IO].unsafeRunSync
  }

  test("read topic from kafka and show aggragation result") {
    topic.description.sparKafka.stats[IO].flatMap(_.dailyHour.show[IO]()).unsafeRunSync
  }

  test("read topic from kafka and show json") {
    val tpk = TopicDef[String, trip_record](
      "nyc_yellow_taxi_trip_data",
      ManualAvroSchema[trip_record](trip_record.schema)).in(ctx)

    tpk.description.sparKafka
      .datasetFromKafka[IO, String](_.asJson.noSpaces)
      .flatMap(_.show[IO](truncate = false, numRows = 1))
      .unsafeRunSync
  }
}
