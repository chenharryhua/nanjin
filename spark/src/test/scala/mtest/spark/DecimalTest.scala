package mtest.spark

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark._
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import frameless.TypedEncoder
import kantan.csv.RowEncoder
import kantan.csv.generic._
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite
import frameless.cats.implicits._

import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode
import scala.util.Random
import cats.implicits._
import io.circe.Codec
import io.circe.generic.auto._
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec

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

  val expected =
    Set(
      Duck(BigDecimal("1234.56"), BigDecimal("1234.567"), 1),
      Duck(BigDecimal("1234.00"), BigDecimal("1234.000"), 2),
      Duck(BigDecimal("1234.01"), BigDecimal("1234.000"), 3)
    )

  val rdd: RDD[Duck] = sparkSession.sparkContext.parallelize(
    List(
      Duck(BigDecimal("1234.56"), BigDecimal("1234.567"), 1),
      Duck(BigDecimal("1234"), BigDecimal("1234"), 2),
      Duck(BigDecimal("1234.005"), BigDecimal("1234.0000001"), 3)
    ))

  val schema: Schema = (new Schema.Parser).parse(schemaText)

  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP

  implicit val avroEncoder: Encoder[Duck] =
    shapeless.cachedImplicit[Encoder[Duck]].withSchema(SchemaFor[Duck](schema))

  implicit val typedEncoder: TypedEncoder[Duck] = shapeless.cachedImplicit
  implicit val rowEncoder: RowEncoder[Duck]     = shapeless.cachedImplicit
  implicit val circeCodec: Codec[Duck]          = io.circe.generic.semiauto.deriveCodec


  implicit val avroDecoder: Decoder[Duck] =
    shapeless.cachedImplicit[Decoder[Duck]].withSchema(SchemaFor[Duck](schema))


}

class DecimalTest extends AnyFunSuite {
  import DecimalTestData._
  test("avro multi/single should be same") {
    val multi  = "./data/test/spark/decimal/avro/multi"
    val single = "./data/test/spark/decimal/avro/single.avro"
    val spark  = "./data/test/spark/decimal/avro/spark.avro"
    import sparkSession.implicits._
  }
  test("parquet multi/single should be same") {
    val multi  = "./data/test/spark/decimal/parquet/multi"
    val single = "./data/test/spark/decimal/parquet/single.parquet"
  }

  test("jackson multi/single should be same") {
    val multi  = "./data/test/spark/decimal/jackson/multi"
    val single = "./data/test/spark/decimal/jackson/single.json"

  }

  test("circe multi/single should be same") {
    val multi  = "./data/test/spark/decimal/circe/multi"
    val single = "./data/test/spark/decimal/circe/single.json"

  }

  test("csv multi/single should be same") {
    val multi  = "./data/test/spark/decimal/csv/multi"
    val single = "./data/test/spark/decimal/csv/single.csv"

  }

}
