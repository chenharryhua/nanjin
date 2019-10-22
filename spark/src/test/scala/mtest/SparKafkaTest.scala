package mtest

import java.time.{LocalDate, LocalDateTime}

import cats.effect.IO
import cats.implicits._
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.database._
import frameless.cats.implicits._
import cats.derived.auto.show._
import java.time.ZoneId

class SparKafkaTest extends AnyFunSuite {
  implicit val zoneId = ZoneId.systemDefault()

  val e     = EmbeddedForTaskSerializable(0, LocalDateTime.now)
  val data  = ForTaskSerializable(0, "a", LocalDate.now, LocalDateTime.now, e)
  val topic = topics.sparkafkaTopic

  (topic.schemaRegistry.register >>
    topic.producer.send(0, data) >>
    topic.producer.send(1, data)).unsafeRunSync()

  test("read topic from kafka") {
    sparKafkaSession.use { s =>
      s.datasetFromKafka(topics.sparkafkaTopic).flatMap(_.consumerRecords.show[IO]())
    }.unsafeRunSync
  }
  test("save topic to disk") {
    sparKafkaSession.use(_.updateParams(_.withOverwrite).saveToDisk(topics.sparkafkaTopic)).unsafeRunSync
  }
  test("read topic from disk") {
    sparKafkaSession
      .use(_.datasetFromDisk(topics.sparkafkaTopic).flatMap(_.consumerRecords.show[IO]()))
      .unsafeRunSync
  }
  test("upload dataset to kafka") {
    sparKafkaSession
      .use(
        _.updateParams(_.withoutTimestamp.withoutPartition)
          .datasetFromDisk(topics.sparkafkaTopic)
          .flatMap(_.toProducerRecords.kafkaUpload(topics.sparkafkaTopic).compile.drain))
      .unsafeRunSync()
  }

  test("read topic from kafka and show values") {
    sparKafkaSession.use { s =>
      s.datasetFromKafka(topics.sparkafkaTopic).flatMap(_.consumerRecords.values.show[IO]())
    }.unsafeRunSync
  }

  test("read topic from kafka and show aggragation result") {
    sparKafkaSession.use { s =>
      s.datasetFromKafka(topics.sparkafkaTopic).flatMap(_.dailyHour.show[IO]())
    }.unsafeRunSync
  }
}
