package mtest.spark.kafka

import java.time.Instant
import java.time.temporal.ChronoUnit

import cats.derived.auto.show._
import cats.effect.IO
import cats.implicits._
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
    topic.sparKafka.fromKafka.flatMap(_.save.partition.avro.multi.run(blocker)).unsafeRunSync()
  }
  test("partition binary avro") {
    topic.sparKafka.fromKafka.flatMap(_.save.partition.binAvro.run(blocker)).unsafeRunSync()
  }
  test("partition jackson") {
    topic.sparKafka.fromKafka.flatMap(_.save.partition.jackson.multi.run(blocker)).unsafeRunSync()
  }
  test("partition parquet") {
    topic.sparKafka.fromKafka.flatMap(_.save.partition.parquet.run(blocker)).unsafeRunSync()
  }
  test("partition circe") {
    topic.sparKafka.fromKafka.flatMap(_.save.partition.circe.run(blocker)).unsafeRunSync()
  }
  test("partition java object") {
    topic.sparKafka.fromKafka.flatMap(_.save.partition.javaObject.run(blocker)).unsafeRunSync()
  }
  test("partition text") {
    topic.sparKafka.fromKafka.flatMap(_.save.partition.text.single.run(blocker)).unsafeRunSync()
  }
}
