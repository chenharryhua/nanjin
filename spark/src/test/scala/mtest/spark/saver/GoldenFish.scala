package mtest.spark.saver

import java.sql.Timestamp
import java.time.Instant

import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
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

  val schema: Schema = (new Schema.Parser).parse(schemaText)

  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP
  val avroCodec: NJAvroCodec[GoldenFish]                   = NJAvroCodec[GoldenFish](schemaText).right.get
  implicit val typedEncoder: TypedEncoder[GoldenFish]      = shapeless.cachedImplicit

  implicit val avroTypedEncoder: AvroTypedEncoder[GoldenFish] =
    AvroTypedEncoder[GoldenFish](avroCodec)

}
