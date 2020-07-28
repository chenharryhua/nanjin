package mtest.spark

import java.sql.Timestamp
import java.time.{Instant, LocalDate}

import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.TypedDataset
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

object CompatTestData {
  final case class Pigeon(canFly: Boolean, legs: Int, weight: Float, now: Timestamp)

  val pigeons: List[Pigeon] =
    List.fill(100)(Pigeon(Random.nextBoolean(), 2, Random.nextFloat(), Timestamp.from(Instant.now)))

}

class CompatTest extends AnyFunSuite {
  import CompatTestData._

  test("spark generated avro can not be consumed by nj") {
    val path = "./data/test/spark/compat/spark.avro"

    val tds = TypedDataset.create(sparkSession.sparkContext.parallelize(pigeons))
    tds.write.mode(SaveMode.Overwrite).format("avro").save(path)
    // val rst = sparkSession.load.avro[Pigeon](path).collect().toSet
    // assert(rst == pigeons.toSet)
  }

  test("spark generated parquet can not be comsumed by nj") {
    val path = "./data/test/spark/compat/spark.parquet"

    val tds = TypedDataset.create(sparkSession.sparkContext.parallelize(pigeons))
    tds.write.mode(SaveMode.Overwrite).parquet(path)

    // val rst = sparkSession.load.parquet[Pigeon](path).collect().toSet
    // assert(rst == pigeons.toSet)
  }

  test("nj generated avro can be consumed by spark - multi") {
    import sparkSession.implicits._
    val path = "./data/test/spark/compat/nj-multi.avro"

    TypedDataset
      .create(sparkSession.sparkContext.parallelize(pigeons))
      .dataset
      .rdd
      .save
      .multi(blocker)
      .avro(path)
      .unsafeRunSync()

    val rst = sparkSession.read.format("avro").load(path).as[Pigeon].collect().toSet

    assert(rst == pigeons.toSet)
  }

  test("nj generated parquet can be consumed by spark - multi") {
    import sparkSession.implicits._
    val path = "./data/test/spark/compat/nj-multi.parquet"

    TypedDataset
      .create(sparkSession.sparkContext.parallelize(pigeons))
      .dataset
      .rdd
      .save
      .multi(blocker)
      .parquet(path)
      .unsafeRunSync()

    val rst = sparkSession.read.parquet(path).as[Pigeon].collect().toSet

    assert(rst == pigeons.toSet)
  }

  test("nj generated avro can be consumed by spark - single") {
    import sparkSession.implicits._
    val path = "./data/test/spark/compat/nj-single.avro"

    TypedDataset
      .create(sparkSession.sparkContext.parallelize(pigeons))
      .dataset
      .rdd
      .save
      .single(blocker)
      .avro(path)
      .unsafeRunSync()

    val rst = sparkSession.read.format("avro").load(path).as[Pigeon].collect().toSet

    assert(rst == pigeons.toSet)
  }

  test("nj generated parquet can be consumed by spark - single") {
    import sparkSession.implicits._
    val path = "./data/test/spark/compat/nj-single.parquet"

    TypedDataset
      .create(sparkSession.sparkContext.parallelize(pigeons))
      .dataset
      .rdd
      .save
      .single(blocker)
      .parquet(path)
      .unsafeRunSync()

    val rst = sparkSession.read.parquet(path).as[Pigeon].collect().toSet

    assert(rst == pigeons.toSet)
  }
}
