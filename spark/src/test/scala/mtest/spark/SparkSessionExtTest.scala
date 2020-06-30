package mtest.spark

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.cats.implicits._
import fs2.Stream
import org.apache.spark.sql.AnalysisException
import org.scalatest.funsuite.AnyFunSuite
import kantan.csv.generic._

object SparkSessionExtTestData {
  final case class Elephant(age: Long, weight: Double, food: List[String])

  val elephants = List(
    Elephant(1, 100.3, List("apple", "orange")),
    Elephant(2, 200.3, List("lemon")),
    Elephant(3, 300.3, List("rice", "leaf", "grass"))
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

    assertThrows[AnalysisException](
      sparkSession.csv[Elephant](path).collect[IO]().unsafeRunSync().toSet)
  }
  test("source shoud be able to read varying lengh csv") {
    val path    = "./data/test/spark/sse/elephant-nj.csv"
    val data    = Stream.emits(elephants)
    val prepare = delete(path) >> data.through(sink.csv[Elephant](path)).compile.drain
    prepare.unsafeRunSync()

    assert(source.csv[Elephant](path).compile.toList.unsafeRunSync().toSet == elephants.toSet)

  }
}
