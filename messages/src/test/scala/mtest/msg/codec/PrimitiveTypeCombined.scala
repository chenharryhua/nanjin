package mtest.msg.codec

import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroFor, KafkaSerde}
import io.circe.Json

final case class PrimitiveTypeCombined(
  a: Int,
  b: Long,
  c: Float,
  d: Double,
  e: String
)
object PrimitiveTypeCombined {
  val primitiviesCodec: KafkaSerde[PrimitiveTypeCombined] =
    AvroFor[PrimitiveTypeCombined].asKey(sr).withTopic(("topic.avro"))

  val jsonPrimCodec: KafkaSerde[Json] =
    AvroFor[Json].asValue(sr).withTopic(("topic.json"))
}
