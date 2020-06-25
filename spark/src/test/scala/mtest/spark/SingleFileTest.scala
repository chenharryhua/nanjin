package mtest.spark

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.spark._
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import io.circe.generic.auto._
import kantan.csv.generic._
import scala.util.Random

object SingleFileTestData {
  final case class Swordfish(from: String, weight: Float, code: Int)

  val fishes =
    List(
      Swordfish("pacific occean", 10.3f, Random.nextInt()),
      Swordfish("india occean", 2.5f, Random.nextInt()),
      Swordfish("atlantic occean", 5.5f, Random.nextInt()))

  val ss: Stream[IO, Swordfish] = Stream.emits(fishes).covary[IO]
}

class SingleFileTest extends AnyFunSuite {
  import SingleFileTestData._
  val sink                 = fileSink[IO](blocker)
  val source               = fileSource[IO](blocker)
  def delete(path: String) = sink.delete(path)
  import sparkSession.implicits._

  test("avro - identity") {
    val path = "./data/test/spark/singleFile/swordfish.avro"
    val run = delete(path) >>
      ss.through(sink.avro[Swordfish](path)).compile.drain >>
      source.avro[Swordfish](path).compile.toList

    assert(run.unsafeRunSync() === fishes)

    val s = sparkSession.read.format("avro").load(path).as[Swordfish].collect().toSet
    assert(s == fishes.toSet)
  }

  test("avro-binary - identity") {
    val path = "./data/test/spark/singleFile/swordfish-binary.avro"
    val run = delete(path) >>
      ss.through(sink.binary[Swordfish](path)).compile.drain >>
      source.binary[Swordfish](path).compile.toList

    assert(run.unsafeRunSync() === fishes)
  }

  test("parquet - identity") {
    val path = "./data/test/spark/singleFile/swordfish.parquet"
    val run = delete(path) >>
      ss.through(sink.parquet[Swordfish](path)).compile.drain >>
      source.parquet[Swordfish](path).compile.toList

    assert(run.unsafeRunSync() === fishes)

    val s = sparkSession.read.parquet(path).as[Swordfish].collect().toSet
    assert(s == fishes.toSet)
  }
  test("json - identity") {
    val path = "./data/test/spark/singleFile/swordfish.json"
    val run = delete(path) >>
      ss.through(sink.json[Swordfish](path)).compile.drain >>
      source.json[Swordfish](path).compile.toList

    assert(run.unsafeRunSync() === fishes)

  }
  test("jackson - identity") {
    val path = "./data/test/spark/singleFile/swordfish-jackson.json"
    val run = delete(path) >>
      ss.through(sink.jackson[Swordfish](path)).compile.drain >>
      source.jackson[Swordfish](path).compile.toList

    assert(run.unsafeRunSync() === fishes)

  }

  test("csv - identity") {
    val path = "./data/test/spark/singleFile/swordfish.csv"
    val run = delete(path) >>
      ss.through(sink.csv[Swordfish](path)).compile.drain >>
      source.csv[Swordfish](path).compile.toList

    assert(run.unsafeRunSync() === fishes)
  }

  test("java-object - identity") {
    val path = "./data/test/spark/singleFile/swordfish.obj"
    val run = delete(path) >>
      ss.through(sink.javaObject[Swordfish](path)).compile.drain >>
      source.javaObject[Swordfish](path).compile.toList
    assert(run.unsafeRunSync() === fishes)
  }
}
