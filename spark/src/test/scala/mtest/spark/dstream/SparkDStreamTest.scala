package mtest.spark.dstream

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJTimestamp}
import com.github.chenharryhua.nanjin.spark.dstream.DStreamRunner
import com.github.chenharryhua.nanjin.spark.kafka.SparKafkaTopic
import fs2.Stream
import fs2.kafka.{ProducerRecord, ProducerRecords}
import io.circe.generic.auto._
import mtest.spark._
import mtest.spark.kafka.sparKafka
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

class SparkDStreamTest extends AnyFunSuite {
  val root: String = "./data/test/spark/dstream/"

  val topic: SparKafkaTopic[IO, Int, String] = sparKafka.topic[Int, String]("dstream.test")

  val sender = Stream
    .awakeEvery[IO](0.1.seconds)
    .zipWithIndex
    .map { case (_, idx) => ProducerRecords.one(ProducerRecord(topic.topicName.value, idx.toInt, "a")) }
    .through(topic.topic.fs2Channel.producerPipe)

  ignore("dstream") {
    val jackson    = root + "jackson/"
    val circe      = root + "circe/"
    val avro       = root + "avro/"
    val checkpoint = root + "checkpont/"

    val runner: DStreamRunner[IO] = DStreamRunner[IO](sparkSession.sparkContext, checkpoint, 3.second)
    sender
      .concurrently(
        runner
          .signup(topic.dstream)(_.jackson(jackson))
          .signup(topic.dstream)(_.circe(circe))
          .signup(topic.dstream)(_.avro(avro))
          .run)
      .interruptAfter(10.seconds)
      .compile
      .drain
      .unsafeRunSync()

    val count = (
      topic.load.jackson(jackson + NJTimestamp.now().`Year=yyyy/Month=mm/Day=dd`(sydneyTime)).count,
      topic.load.avro(avro + NJTimestamp.now().`Year=yyyy/Month=mm/Day=dd`(sydneyTime)).count,
      topic.load.circe(circe + NJTimestamp.now().`Year=yyyy/Month=mm/Day=dd`(sydneyTime)).count).mapN((_, _, _))
    println(count.unsafeRunSync())
  }

  ignore("dstream - 2") {
    val jackson    = root + "jackson2/"
    val circe      = root + "circe2/"
    val avro       = root + "avro2/"
    val checkpoint = root + "checkpont2/"

    val runner: DStreamRunner[IO] = DStreamRunner[IO](sparkSession.sparkContext, checkpoint, 3.second)
    sender
      .concurrently(
        runner
          .signup(topic.dstream) { ds =>
            ds.coalesce.jackson(jackson)
            ds.coalesce.avro(avro)
            ds.coalesce.circe(circe)
          }
          .run)
      .interruptAfter(10.seconds)
      .compile
      .drain
      .unsafeRunSync()

    val count = (
      topic.load.jackson(jackson + NJTimestamp.now().`Year=yyyy/Month=mm/Day=dd`(sydneyTime)).count,
      topic.load.avro(avro + NJTimestamp.now().`Year=yyyy/Month=mm/Day=dd`(sydneyTime)).count,
      topic.load.circe(circe + NJTimestamp.now().`Year=yyyy/Month=mm/Day=dd`(sydneyTime)).count).mapN((_, _, _))
    println(count.unsafeRunSync())
  }
}
