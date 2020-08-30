package mtest.spark.persist

import java.sql.Timestamp
import java.time.Instant

import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.injection._
import com.sksamuel.avro4s.{Decoder, Encoder}
import frameless.TypedEncoder
import org.apache.avro.Schema

import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode

final case class GoldenFish(a: Instant, b: Timestamp, c: BigDecimal)

object GoldenFish {

  val schemaText: String =
    """
      |{
      |  "type": "record",
      |  "name": "GoldenFish",
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

  val schema: Schema = (new Schema.Parser).parse(schemaText)

  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP
  implicit val avroEncoder: Encoder[GoldenFish]            = shapeless.cachedImplicit
  implicit val avroDecoder: Decoder[GoldenFish]            = shapeless.cachedImplicit

  implicit val typedEncoder: TypedEncoder[GoldenFish] = shapeless.cachedImplicit

  implicit val avroCodec: NJAvroCodec[GoldenFish] =
    NJAvroCodec[GoldenFish](schema)(avroDecoder, avroEncoder).right.get

  implicit val avroTypedEncoder: AvroTypedEncoder[GoldenFish] =
    AvroTypedEncoder[GoldenFish](avroCodec)

  println(avroTypedEncoder.sparkDatatype)
  println(avroTypedEncoder.sparkAvroSchema)

}
