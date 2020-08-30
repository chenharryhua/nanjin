package mtest.spark

import java.time.LocalDateTime

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.cats.implicits._
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import kantan.csv.generic._
import kantan.csv.java8._
import kantan.csv.RowEncoder
import kantan.csv.RowDecoder
import com.sksamuel.avro4s.Encoder

object SingleWriteSparkReadTestData {
  final case class Elephant(birthDay: LocalDateTime, weight: Double, food: List[String])

  val elephants = List(
    Elephant(LocalDateTime.of(2020, 6, 1, 12, 10, 10), 100.3, List("apple", "orange")),
    Elephant(LocalDateTime.of(2021, 6, 1, 12, 10, 10), 200.3, List("lemon")),
    Elephant(LocalDateTime.of(2022, 6, 1, 12, 10, 10), 300.3, List("rice", "leaf", "grass"))
  )
}

class SingleWriteSparkReadTest extends AnyFunSuite {
  import SingleWriteSparkReadTestData._
  val sink                 = fileSink[IO](blocker)
  val source               = fileSource[IO](blocker)
  def delete(path: String) = sink.delete(path)
  implicit val zoneId      = sydneyTime

  test("spark source is able to read varying length csv") {
    val path = "./data/test/spark/sse/elephant-spark.csv"
    val data = Stream.emits(elephants)
    val prepare =
      delete(path) >> data.through(sink.csv[Elephant](path)).compile.drain
    prepare.unsafeRunSync()

    val rst = fileSource[IO](blocker).csv[Elephant](path).compile.toList.unsafeRunSync()
    assert(rst.toSet == elephants.toSet)
  }
  test("spark is unable to read varying lengh csv") {
    val path = "./data/test/spark/sse/elephant-nj.csv"
    val data = Stream.emits(elephants)
    val prepare =
      delete(path) >> data.through(sink.csv[Elephant](path)).compile.drain
    prepare.unsafeRunSync()
  }

  test("spark avro read/write identity") {
    val path = "./data/test/spark/sse/elephant.avro"
    val data = Stream.emits(elephants)
    val prepare =
      delete(path) >> data.through(sink.avro[Elephant](path)).compile.drain
    prepare.unsafeRunSync()
  }
}
