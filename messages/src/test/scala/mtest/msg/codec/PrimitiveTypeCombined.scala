package mtest.msg.codec

import com.github.chenharryhua.nanjin.messages.kafka.codec.{KJson, NJCodec, SerdeOf}
import io.circe.generic.auto.*
final case class PrimitiveTypeCombined(
  a: Int,
  b: Long,
  c: Float,
  d: Double,
  e: String
)
object PrimitiveTypeCombined {
  val primitiviesCodec: NJCodec[PrimitiveTypeCombined] =
    SerdeOf[PrimitiveTypeCombined].asKey(sr).codec("topic.avro")

  val jsonPrimCodec: NJCodec[KJson[PrimitiveTypeCombined]] =
    SerdeOf[KJson[PrimitiveTypeCombined]].asValue(sr).codec("topic.json")
}
