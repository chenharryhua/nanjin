package mtest.spark.kafka

import java.time.Instant
import java.time.temporal.ChronoUnit

import cats.derived.auto.show._
import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef, TopicName}
import com.github.chenharryhua.nanjin.spark.kafka._
import fs2.kafka.ProducerRecord
import io.circe.generic.auto._
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

object PartitionTestData {
  final case class Shark(a: Int, b: String)

  val today: Long     = Instant.now().toEpochMilli
  val yesterday: Long = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli

  val topic: KafkaTopic[IO, Int, Shark] =
    TopicDef[Int, Shark](TopicName("test.spark.kafka.partition")).in(ctx)

  val yt: List[ProducerRecord[Int, Shark]] = List.fill(50)(
    topic.fs2PR(Random.nextInt(), Shark(Random.nextInt(), "yesterday")).withTimestamp(yesterday))

  val tt: List[ProducerRecord[Int, Shark]] = List.fill(50)(
    topic.fs2PR(Random.nextInt(), Shark(Random.nextInt(), "today")).withTimestamp(today))

  val data: List[ProducerRecord[Int, Shark]] = yt ++ tt
}

class PartitionTest extends AnyFunSuite {
  import PartitionTestData._
  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
    topic.send(data)).unsafeRunSync()
  test("partition avro") {
    topic.sparKafka.fromKafka.flatMap(_.partition.folder.avro.run(blocker)).unsafeRunSync()
  }
  test("partition binary avro") {
    topic.sparKafka.fromKafka.flatMap(_.partition.binAvro.run(blocker)).unsafeRunSync()
  }
  test("partition jackson") {
    topic.sparKafka.fromKafka.flatMap(_.partition.folder.jackson.run(blocker)).unsafeRunSync()
  }
  test("partition parquet") {
    topic.sparKafka.fromKafka.flatMap(_.partition.parquet.run(blocker)).unsafeRunSync()
  }
  test("partition circe") {
    topic.sparKafka.fromKafka.flatMap(_.partition.circe.run(blocker)).unsafeRunSync()
  }
  test("partition text") {
    topic.sparKafka.fromKafka.flatMap(_.partition.file.text.run(blocker)).unsafeRunSync()
  }
}
