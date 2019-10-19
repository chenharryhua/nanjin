package mtest

import java.time.{LocalDate, LocalDateTime}

import cats.effect.IO
import cats.implicits._
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.sparkafka._
import frameless.cats.implicits._
import cats.derived.auto.show._

class SparKafkaTest extends AnyFunSuite {

  val e     = EmbeddedForTaskSerializable(0, LocalDateTime.now)
  val data  = ForTaskSerializable(0, "a", LocalDate.now, LocalDateTime.now, e)
  val topic = topics.sparkafkaTopic

  (topic.schemaRegistry.register >>
    topic.producer.send(0, data) >>
    topic.producer.send(1, data)).unsafeRunSync()

  test("read topic from kafka") {
    spark.use { s =>
      s.datasetFromKafka(topics.sparkafkaTopic).flatMap(_.consumerRecords.show[IO]())
    }.unsafeRunSync
  }
  test("save topic to disk") {
    spark.use(_.update(_.withOverwrite).saveToDisk(topics.sparkafkaTopic)).unsafeRunSync
  }
  test("read topic from disk") {
    spark
      .use(_.datasetFromDisk(topics.sparkafkaTopic).flatMap(_.consumerRecords.show[IO]()))
      .unsafeRunSync
  }
  test("upload dataset to kafka") {
    spark
      .use(
        _.datasetFromDisk(topics.sparkafkaTopic).flatMap(
          _.toProducerRecords.kafkaUpload(topics.sparkafkaTopic).compile.drain))
      .unsafeRunSync()
  }
  test("replay topic using data on disk") {
    spark.use(_.replay(topics.sparkafkaTopic))
  }
}
