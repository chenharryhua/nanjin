package mtest.spark

import java.sql.Timestamp
import java.time.{Instant, LocalDate}

import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.TypedDataset
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

object CompatTestData {
  final case class Pigeon(canFly: Boolean, legs: Int, birth: LocalDate)

  val pigeons: List[Pigeon] =
    List.fill(100)(Pigeon(Random.nextBoolean(), 2, LocalDate.of(2020, 7, 28)))

}

class CompatTest extends AnyFunSuite {
  import CompatTestData._

  test("avro") {
    val path = "./data/test/spark/compat/multi.avro"

    val tds = TypedDataset.create(sparkSession.sparkContext.parallelize(pigeons))
    tds.write.mode(SaveMode.Overwrite).format("avro").save(path)
    val rst = sparkSession.load.avro[Pigeon](path).collect().toSet
    assert(rst == pigeons.toSet)
  }
  test("parquet") {
    val path = "./data/test/spark/compat/multi.parquet"

    val tds = TypedDataset.create(sparkSession.sparkContext.parallelize(pigeons))
    tds.write.mode(SaveMode.Overwrite).parquet(path)
    val rst = sparkSession.load.parquet[Pigeon](path).collect().toSet
    assert(rst == pigeons.toSet)
  }
  test("csv") {
    val path = "./data/test/spark/compat/multi.csv"

    val tds = TypedDataset.create(sparkSession.sparkContext.parallelize(pigeons))
    tds.write.mode(SaveMode.Overwrite).csv(path)
    val rst = sparkSession.load.csv[Pigeon](path).collect().toSet
    assert(rst == pigeons.toSet)
  }
}
