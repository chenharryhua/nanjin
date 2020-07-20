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

object SparkSessionExtTestData {
  final case class Elephant(birthDay: LocalDateTime, weight: Double, food: List[String])

  val elephants = List(
    Elephant(LocalDateTime.of(2020, 6, 1, 12, 10, 10), 100.3, List("apple", "orange")),
    Elephant(LocalDateTime.of(2021, 6, 1, 12, 10, 10), 200.3, List("lemon")),
    Elephant(LocalDateTime.of(2022, 6, 1, 12, 10, 10), 300.3, List("rice", "leaf", "grass"))
  )
}

class SparkSessionExtTest extends AnyFunSuite {
  import SparkSessionExtTestData._
  val sink                 = fileSink[IO](blocker)
  val source               = fileSource[IO](blocker)
  def delete(path: String) = sink.delete(path)
  implicit val zoneId      = sydneyTime

  test("spark can not process varying length csv -- hope it fails someday") {
    val path    = "./data/test/spark/sse/elephant-spark.csv"
    val data    = Stream.emits(elephants)
    val prepare = delete(path) >> data.through(sink.csv[Elephant](path)).compile.drain
    prepare.unsafeRunSync()

    assertThrows[Exception](sparkSession.csv[Elephant](path).collect[IO]().unsafeRunSync().toSet)
  }
  test("spark source shoud be able to read varying lengh csv") {
    val path    = "./data/test/spark/sse/elephant-nj.csv"
    val data    = Stream.emits(elephants)
    val prepare = delete(path) >> data.through(sink.csv[Elephant](path)).compile.drain
    prepare.unsafeRunSync()

    assert(source.csv[Elephant](path).compile.toList.unsafeRunSync().toSet == elephants.toSet)
  }

  test("spark avro read/write identity") {
    val path = "./data/test/spark/sse/elephant.avro"
    val data = Stream.emits(elephants)
    (delete(path) >> data.through(sink.avro[Elephant](path)).compile.drain).unsafeRunSync()
    val rst = sparkSession.avro[Elephant](path).collect.toSet
    assert(rst == elephants.toSet)
  }

  test("spark parquet read/write identity") {
    val path = "./data/test/spark/sse/elephant.parquet"
    val data = Stream.emits(elephants)
    (delete(path) >> data.through(sink.parquet[Elephant](path)).compile.drain).unsafeRunSync()
    val rst = sparkSession.parquet[Elephant](path).collect.toSet
    assert(rst == elephants.toSet)
  }
}
