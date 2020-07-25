package mtest.spark

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.{fileSink, _}
import com.github.chenharryhua.nanjin.spark.injection._
import io.circe.generic.auto._
import io.circe.shapes._
import org.scalatest.funsuite.AnyFunSuite
import frameless.cats.implicits._
import scala.util.Random

object SimpleFormatTestData {
  final case class Simple(a: Int, b: String, c: Float, d: Double)

  val simple: List[Simple] =
    List.fill(300)(Simple(Random.nextInt(), "a", Random.nextFloat(), Random.nextDouble()))
}

class SimpleFormatTest extends AnyFunSuite {
  import SimpleFormatTestData._
  test("avro read/write identity") {
    val single = "./data/test/spark/simple/avro/single.avro"
    val multi  = "./data/test/spark/simple/avro/multi.avro"
    val rdd    = sparkSession.sparkContext.parallelize(simple)
    val prepare = fileSink[IO](blocker).delete(single) >>
      rdd.single[IO](blocker).avro(single) >>
      fileSink[IO](blocker).delete(multi) >>
      rdd.multi[IO](blocker).avro(multi)
    prepare.unsafeRunSync()

    assert(sparkSession.avro[Simple](single).collect().toSet == simple.toSet)
    assert(sparkSession.avro[Simple](multi).collect().toSet == simple.toSet)
  }

  test("jackson read/write identity") {
    val single = "./data/test/spark/simple/jackson/jackson.json"
    val multi  = "./data/test/spark/simple/jackson/multi.jackson"
    val rdd    = sparkSession.sparkContext.parallelize(simple)
    val prepare = rdd.single[IO](blocker).jackson(single) >>
      rdd.multi[IO](blocker).jackson(multi)
    prepare.unsafeRunSync()

    assert(sparkSession.jackson[Simple](single).collect().toSet == simple.toSet)
    assert(sparkSession.jackson[Simple](multi).collect().toSet == simple.toSet)
  }

  test("circe read/write identity") {
    val single = "./data/test/spark/simple/circe/circe.json"
    val multi  = "./data/test/spark/simple/circe/multi.circe"

    val rdd = sparkSession.sparkContext.parallelize(simple)
    val prepare = rdd.single[IO](blocker).circe(single) >>
      rdd.multi[IO](blocker).circe(multi)
    prepare.unsafeRunSync()

    assert(sparkSession.circe[Simple](single).collect().toSet == simple.toSet)
    assert(sparkSession.circe[Simple](multi).collect().toSet == simple.toSet)
  }

  test("parquet read/write identity") {
    val single = "./data/test/spark/simple/parquet/single.parquet"
    val multi  = "./data/test/spark/simple/parquet/multi.parquet"

    val rdd = sparkSession.sparkContext.parallelize(simple)
    val prepare = rdd.single[IO](blocker).parquet(single) >>
      rdd.multi[IO](blocker).parquet(multi)
    prepare.unsafeRunSync()

    assert(sparkSession.parquet[Simple](single).collect[IO]().unsafeRunSync().toSet == simple.toSet)
    assert(sparkSession.parquet[Simple](multi).collect[IO]().unsafeRunSync().toSet == simple.toSet)
  }
}
