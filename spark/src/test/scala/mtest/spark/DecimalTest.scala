package mtest.spark

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.saver.RddFileSaver
import com.sksamuel.avro4s.Encoder
import frameless.TypedEncoder
import kantan.csv.RowEncoder
import kantan.csv.generic._
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

object DecimalTestData {
  final case class Duck(a: BigDecimal, b: BigDecimal, c: Int)

  val schemaText: String =
    """
      |{
      |  "type": "record",
      |  "name": "Duck",
      |  "namespace": "nj.spark",
      |  "fields": [
      |    {
      |      "name": "a",
      |      "type": {
      |        "type": "bytes",
      |        "logicalType": "decimal",
      |        "precision": 6,
      |        "scale": 2
      |      }
      |    },
      |    {
      |      "name": "b",
      |      "type": {
      |        "type": "bytes",
      |        "logicalType": "decimal",
      |        "precision": 7,
      |        "scale": 3
      |      }
      |    },
      |    {
      |      "name": "c",
      |      "type": "int"
      |    }
      |  ]
      |}
      |""".stripMargin

  val goodData: RDD[Duck] = sparkSession.sparkContext.parallelize(
    List(
      Duck(BigDecimal("1234.56"), BigDecimal("1234.567"), Random.nextInt()),
      Duck(BigDecimal("1234"), BigDecimal("12.34567"), Random.nextInt()),
      Duck(BigDecimal("1234.00"), BigDecimal("1234.0000001"), Random.nextInt())
    ))

  val schema: Schema = (new Schema.Parser).parse(schemaText)

  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP

  implicit val avroEncoder: Encoder[Duck]       = shapeless.cachedImplicit
  implicit val typedEncoder: TypedEncoder[Duck] = shapeless.cachedImplicit
  implicit val rowEncoder: RowEncoder[Duck]     = shapeless.cachedImplicit

  val saver = new RddFileSaver[IO, Duck](goodData)

}

class DecimalTest extends AnyFunSuite {
  import DecimalTestData._
  test("jackson multi") {
    saver
      .jackson("./data/test/spark/decimal/jackson/multi")
      .repartition(1)
      .multi
      .run(blocker)
      .unsafeRunSync()
  }
  test("jackson single") {
    saver
      .jackson("./data/test/spark/decimal/jackson/single.json")
      .single
      .run(blocker)
      .unsafeRunSync()
  }

  test("csv multi") {
    saver
      .csv("./data/test/spark/decimal/csv/multi")
      .multi
      .repartition(1)
      .withSchema(schema)
      .run(blocker)
      .unsafeRunSync()
  }
  test("csv single") {
    saver
      .csv("./data/test/spark/decimal/csv/single.csv")
      .single
      .withSchema(schema)
      .run(blocker)
      .unsafeRunSync()
  }
}
