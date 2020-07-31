package mtest.spark

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.spark.{fileSink, _}
import com.github.chenharryhua.nanjin.spark.injection._
import io.circe.generic.auto._
import io.circe.shapes._
import org.scalatest.funsuite.AnyFunSuite
import frameless.cats.implicits._

import scala.util.Random

object SimpleFormatTestData {
  final case class Simple(a: Int, b: String, c: Float, d: Double, instant: Instant)

  val simple: List[Simple] =
    List.fill(300)(
      Simple(Random.nextInt(), "a", Random.nextFloat(), Random.nextDouble(), Instant.now))
}

class SimpleFormatTest extends AnyFunSuite {
  import SimpleFormatTestData._
  test("avro read/write identity") {
    val single = "./data/test/spark/simple/avro/single.avro"
    val multi  = "./data/test/spark/simple/avro/multi.avro"
    val rdd    = sparkSession.sparkContext.parallelize(simple)
    val prepare =
      rdd.save[IO].avro(single).single.overwrite.run(blocker) >>
        rdd.save[IO].avro(multi).multi.overwrite.run(blocker)
    prepare.unsafeRunSync()

    assert(sparkSession.load.avro[Simple](single).collect().toSet == simple.toSet)
    assert(sparkSession.load.avro[Simple](multi).collect().toSet == simple.toSet)
  }

  test("jackson read/write identity") {
    val single = "./data/test/spark/simple/jackson/jackson.json"
    val multi  = "./data/test/spark/simple/jackson/multi.jackson"
    val rdd    = sparkSession.sparkContext.parallelize(simple)
    val prepare = rdd.save[IO].jackson(single).single.run(blocker) >> rdd.save[IO]
      .jackson(multi)
      .multi
      .run(blocker)
    prepare.unsafeRunSync()

    assert(sparkSession.load.jackson[Simple](single).collect().toSet == simple.toSet)
    assert(sparkSession.load.jackson[Simple](multi).collect().toSet == simple.toSet)
  }

  test("circe read/write identity") {
    val single = "./data/test/spark/simple/circe/circe.json"
    val multi  = "./data/test/spark/simple/circe/multi.circe"

    val rdd = sparkSession.sparkContext.parallelize(simple)
    val prepare =
      rdd.save[IO].circe(single).single.run(blocker) >> rdd.save[IO].circe(multi).multi.run(blocker)
    prepare.unsafeRunSync()

    assert(sparkSession.load.circe[Simple](single).collect().toSet == simple.toSet)
    assert(sparkSession.load.circe[Simple](multi).collect().toSet == simple.toSet)
  }

  test("parquet read/write identity") {
    val single = "./data/test/spark/simple/parquet/single.parquet"
    val multi  = "./data/test/spark/simple/parquet/multi.parquet"

    val rdd = sparkSession.sparkContext.parallelize(simple)
    val prepare = rdd.save[IO].parquet(single).single.run(blocker) >> rdd.save[IO]
      .parquet(multi)
      .multi
      .run(blocker)
    prepare.unsafeRunSync()

    assert(sparkSession.load.parquet[Simple](single).collect().toSet == simple.toSet)
    assert(sparkSession.load.parquet[Simple](multi).collect().toSet == simple.toSet)
  }

  test("csv read/write identity") {
    import kantan.csv.generic._
    import kantan.csv.java8._
    val single = "./data/test/spark/simple/csv/single.csv"
    val multi  = "./data/test/spark/simple/csv/multi.csv"
    val rdd    = sparkSession.sparkContext.parallelize(simple)
    val prepare =
      rdd.save[IO].csv(single).single.run(blocker) >> rdd.save[IO].csv(multi).multi.run(blocker)
    prepare.unsafeRunSync()

    assert(sparkSession.load.csv[Simple](single).collect().toSet == simple.toSet)
    //  assert(sparkSession.csv[Simple](multi).collect().toSet == simple.toSet)
  }

  test("text write") {
    import cats.derived.auto.show._
    val single = "./data/test/spark/simple/text/simple.txt"
    val multi  = "./data/test/spark/simple/text/multi.txt"

    val rdd = sparkSession.sparkContext.parallelize(simple)
    val prepare =
      rdd.save[IO].text(single).single.run(blocker) >> rdd.save[IO].text(multi).multi.run(blocker)
    prepare.unsafeRunSync()

    assert(sparkSession.load.text(single).count == simple.size)
    assert(sparkSession.load.text(multi).count == simple.size)
  }
}
