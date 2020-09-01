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

final case class Rooster(
  index: Int,
  a: Instant,
  b: Timestamp,
  c: BigDecimal,
  d: Option[Int] = Some(1))

object Rooster {

  val schemaText: String =
    """
      |{
      |  "type": "record",
      |  "name": "Rooster",
      |  "namespace": "mtest.spark.persist",
      |  "doc": "test save and load data time type",
      |  "fields": [
      |    {
      |      "name": "index",
      |      "type": "int"
      |    },
      |    {
      |      "name": "a",
      |      "type": {
      |        "type": "long",
      |        "logicalType": "timestamp-millis"
      |      }
      |    },
      |    {
      |      "name": "b",
      |      "type": {
      |        "type": "long",
      |        "logicalType": "timestamp-millis"
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
      |    },
      |    { "name":"d", "type":["int","null"]
      |    }
      |  ]
      |}
      |""".stripMargin

  val schema: Schema = (new Schema.Parser).parse(schemaText)

  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP
  implicit val avroEncoder: Encoder[Rooster]               = shapeless.cachedImplicit
  implicit val avroDecoder: Decoder[Rooster]               = shapeless.cachedImplicit

  implicit val typedEncoder: TypedEncoder[Rooster] = shapeless.cachedImplicit

  implicit val avroCodec: NJAvroCodec[Rooster] =
    NJAvroCodec[Rooster](schema).right.get

  implicit val ate: AvroTypedEncoder[Rooster] =
    AvroTypedEncoder[Rooster](TypedEncoder[Rooster], avroCodec)
}
