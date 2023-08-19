package mtest.msg.codec

import com.github.chenharryhua.nanjin.messages.kafka.codec.{KJson, KafkaSerde, SerdeOf}
import io.circe.generic.auto.*
final case class PrimitiveTypeCombined(
  a: Int,
  b: Long,
  c: Float,
  d: Double,
  e: String
)
object PrimitiveTypeCombined {
  val primitiviesCodec: KafkaSerde[PrimitiveTypeCombined] =
    SerdeOf[PrimitiveTypeCombined].asKey(sr).topic("topic.avro")

  val jsonPrimCodec: KafkaSerde[KJson[PrimitiveTypeCombined]] =
    SerdeOf[KJson[PrimitiveTypeCombined]].asValue(sr).topic("topic.json")
}
