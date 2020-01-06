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
import fs2.kafka.producerResource
import fs2.kafka.ProducerRecords
import fs2.kafka.ProducerRecord
import fs2.Chunk
import java.time.Instant

class SparKafkaTest extends AnyFunSuite {
  val e     = EmbeddedForTaskSerializable(0, "embeded")
  val data  = ForTaskSerializable(0, "a", LocalDate.now, Instant.now, e)
  val topic = ctx.topic[Int, ForTaskSerializable]("serializable.test")
  (topic.schemaRegistry.register >> topic.send(List(0 -> data, 1 -> data))).unsafeRunSync()

  test("read topic from kafka") {
    topic.sparKafka.datasetFromKafka[IO].flatMap(_.show[IO]()).unsafeRunSync
  }
  test("save topic to disk") {
    topic.sparKafka.updateParams(_.withOverwrite).saveToDisk[IO].unsafeRunSync
  }
  test("read topic from disk") {
    topic.sparKafka.datasetFromDisk.show[IO]().unsafeRunSync
  }

  test("replay") {
    topic.sparKafka.replay[IO].compile.toList.map(println).unsafeRunSync
  }

  test("read topic from kafka and show aggragation result") {
    topic.sparKafka.stats[IO].flatMap(_.dailyHour.show[IO]()).unsafeRunSync
  }

  test("read topic from kafka and show json") {
    val tpk = TopicDef[trip_record, trip_record]("nyc_yellow_taxi_trip_data").in(ctx)
    tpk.sparKafka.jsonDatasetFromKafka[IO].flatMap(_.show[IO](truncate = false)).unsafeRunSync
  }
}
