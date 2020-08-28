package mtest.spark

import java.sql.Timestamp
import java.time.Instant

import cats.effect.IO
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.injection._
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import frameless.{TypedDataset, TypedEncoder}
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite

import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode

object AvroTypedEncoderTestData {

  val schemaText =
    """
      |{
      |  "type": "record",
      |  "name": "Goldenfish",
      |  "namespace": "mtest.spark.AvroTypedEncoderTestData",
      |  "fields": [
      |    {
      |      "name": "a",
      |      "type": {
      |        "type": "long",
      |        "logicalType": "timestamp-micros"
      |      }
      |    },
      |    {
      |      "name": "b",
      |      "type": {
      |        "type": "long",
      |        "logicalType": "timestamp-micros"
      |      }
      |    },
      |    {
      |      "name": "c",
      |      "type": {
      |        "type": "bytes",
      |        "logicalType": "decimal",
      |        "precision": 7,
      |        "scale": 3
      |      }
      |    }
      |  ]
      |}
      |""".stripMargin

  val schema = (new Schema.Parser).parse(schemaText)
  final case class Goldenfish(a: Instant, b: Timestamp, c: BigDecimal)

  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP

  val avroTypedEncoder: AvroTypedEncoder[Goldenfish] =
    new AvroTypedEncoder[Goldenfish](
      TypedEncoder[Goldenfish],
      NJAvroCodec[Goldenfish](schemaText).right.get)

  val data = Goldenfish(Instant.now, Timestamp.from(Instant.now()), BigDecimal("1234.56789"))
  val rdd  = sparkSession.sparkContext.parallelize(List(data))
}

class AvroTypedEncoderTest extends AnyFunSuite {
  import AvroTypedEncoderTestData._
  test("att") {
    implicit val encode: TypedEncoder[Goldenfish] = avroTypedEncoder.sparkEncoder
    println(avroTypedEncoder.sparkDatatype)
    val tds = avroTypedEncoder.typedDataset(rdd, sparkSession)
    tds.select(tds('c)).show[IO](truncate = false).unsafeRunSync()
    TypedDataset.create(rdd).show[IO](truncate = false).unsafeRunSync()
  }
}
