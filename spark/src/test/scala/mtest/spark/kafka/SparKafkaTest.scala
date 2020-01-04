package mtest.spark.kafka

import java.time.{LocalDate, LocalDateTime}

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
import java.time.ZoneId
import io.circe.generic.auto._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.kafka.TopicDef

class SparKafkaTest extends AnyFunSuite {
  val e        = EmbeddedForTaskSerializable(0, LocalDateTime.now)
  val data     = ForTaskSerializable(0, "a", LocalDate.now, LocalDateTime.now, e)
  val topic    = ctx.topic[Int, ForTaskSerializable]("serializable.test")
  val producer = topic.producer[IO]
  (topic.schemaRegistry[IO].register >>
    producer.send(0, data) >>
    producer.send(1, data)).unsafeRunSync()

  test("read topic from kafka") {
    sparKafkaSession
      .datasetFromKafka[IO, Int, ForTaskSerializable](topic.topicDesc)
      .flatMap(_.show[IO]())
      .unsafeRunSync
  }
  test("save topic to disk") {
    sparKafkaSession
      .updateParams(_.withOverwrite)
      .saveToDisk[IO, Int, ForTaskSerializable](topic.topicDesc)
      .unsafeRunSync
  }
  test("read topic from disk") {
    sparKafkaSession
      .datasetFromDisk[IO, Int, ForTaskSerializable](topic.topicDesc)
      .flatMap(_.show[IO]())
      .unsafeRunSync
  }
  test("upload dataset to kafka") {
    sparKafkaSession
      .updateParams(_.withoutTimestamp.withoutPartition)
      .datasetFromDisk[IO, Int, ForTaskSerializable](topic.topicDesc)
      .flatMap(_.toProducerRecords.kafkaUpload[IO](topic.topicDesc).take(5).compile.drain)
      .unsafeRunSync()
  }

  test("read topic from kafka and show aggragation result") {
    sparKafkaSession
      .stats[IO, Int, ForTaskSerializable](topic.topicDesc)
      .flatMap(_.dailyHour.show[IO]())
      .unsafeRunSync
  }

  test("read topic from kafka and show json") {
    val tpk = TopicDef[trip_record, trip_record]("nyc_yellow_taxi_trip_data").in(ctx)
    sparKafkaSession
      .jsonFromKafka[IO, Int, ForTaskSerializable](topic.topicDesc)
      .flatMap(_.show[IO](truncate = false))
      .unsafeRunSync
  }
}
