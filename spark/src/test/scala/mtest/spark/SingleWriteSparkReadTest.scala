package mtest.spark

import java.time.LocalDateTime

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.persist.{
  fileSink,
  fileSource,
  SingleFileSink,
  SingleFileSource
}
import frameless.cats.implicits._
import fs2.Stream
import kantan.csv.{RowDecoder, RowEncoder}
import kantan.csv.generic._
import kantan.csv.java8._
import org.apache.avro.file.CodecFactory
import org.scalatest.funsuite.AnyFunSuite

object SingleWriteSparkReadTestData {
  final case class Elephant(birthDay: LocalDateTime, weight: Double, food: List[String])

  val elephants = List(
    Elephant(LocalDateTime.of(2020, 6, 1, 12, 10, 10), 100.3, List("apple", "orange")),
    Elephant(LocalDateTime.of(2021, 6, 1, 12, 10, 10), 200.3, List("lemon")),
    Elephant(LocalDateTime.of(2022, 6, 1, 12, 10, 10), 300.3, List("rice", "leaf", "grass"))
  )
  implicit val rd: RowDecoder[Elephant] = shapeless.cachedImplicit
  implicit val re: RowEncoder[Elephant] = shapeless.cachedImplicit

}

class SingleWriteSparkReadTest extends AnyFunSuite {
  import SingleWriteSparkReadTestData._
  val sink: SingleFileSink[IO]          = fileSink[IO](blocker)
  val source: SingleFileSource[IO]      = fileSource[IO](blocker)
  def delete(path: String): IO[Boolean] = sink.delete(path)

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
      delete(path) >> data
        .through(sink.avro[Elephant](path, CodecFactory.deflateCodec(6)))
        .compile
        .drain
    prepare.unsafeRunSync()
  }
}
