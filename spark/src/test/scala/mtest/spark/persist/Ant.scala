package mtest.spark.persist

import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.sksamuel.avro4s.{Decoder, Encoder}

final case class Legs(right: String, left: Int)
final case class Ant(a: List[Int], b: Vector[Legs])

object Ant {

  val schemaText: String =
    """
      |
      |{
      |  "type": "record",
      |  "name": "Ant",
      |  "namespace": "mtest.spark.persist",
      |  "doc": "test save and load collection type",
      |  "fields": [
      |    {
      |      "name": "a",
      |      "type": {
      |        "type": "array",
      |        "items": "int"
      |      }
      |    },
      |    {
      |      "name": "b",
      |      "type": {
      |        "type": "array",
      |        "items": {
      |          "type": "record",
      |          "name": "Legs",
      |          "fields": [
      |            {
      |              "name": "right",
      |              "type": "string"
      |            },
      |            {
      |              "name": "left",
      |              "type": "int"
      |            }
      |          ]
      |        }
      |      }
      |    }
      |  ]
      |}
      |
      |""".stripMargin

  val avroEncoder: Encoder[Ant] = shapeless.cachedImplicit
  val avroDecoder: Decoder[Ant] = shapeless.cachedImplicit

  val avroCodec: AvroCodec[Ant] = AvroCodec[Ant](schemaText)
}
