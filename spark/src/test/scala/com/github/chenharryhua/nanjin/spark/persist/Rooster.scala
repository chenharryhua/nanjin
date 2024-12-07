package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import frameless.TypedEncoder
import io.circe.Codec
import kantan.csv.RowEncoder
import org.apache.avro.Schema

import java.sql.Timestamp
import java.time.Instant
import scala.math.BigDecimal.RoundingMode

final case class Rooster(index: Int, a: Instant, b: Timestamp, c: BigDecimal, d: BigDecimal, e: Option[Int])

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
      |      "type": "int",
      |      "doc": "the doc should not be discarded by avro codec"
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
      |    },
      |    {
      |      "name": "d",
      |      "type": {
      |        "type": "bytes",
      |        "logicalType": "decimal",
      |        "precision": 6,
      |        "scale": 0
      |      }
      |    },
      |    { "name":"e", "type":["int","null"]
      |    }
      |  ]
      |}
      |""".stripMargin

  val schema: Schema = (new Schema.Parser).parse(schemaText)

  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP

  implicit val circeCodec: Codec[Rooster] = io.circe.generic.semiauto.deriveCodec

  implicit val typedEncoder: TypedEncoder[Rooster] = shapeless.cachedImplicit

  val avroCodec: AvroCodec[Rooster] = AvroCodec[Rooster](schema)

  val ate: AvroTypedEncoder[Rooster] =
    AvroTypedEncoder[Rooster](TypedEncoder[Rooster], avroCodec)

  implicit val showRooster: Show[Rooster] = _.toString

  implicit val rowEncoderRooster: RowEncoder[Rooster] = (d: Rooster) =>
    List(d.index.toString, d.a.toString, d.b.toString)

}
