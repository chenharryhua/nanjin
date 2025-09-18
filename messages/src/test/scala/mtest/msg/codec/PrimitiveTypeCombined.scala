package mtest.msg.codec

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroFor, KJson, KafkaSerde}
import io.circe.generic.auto.*
import eu.timepit.refined.auto.*

final case class PrimitiveTypeCombined(
  a: Int,
  b: Long,
  c: Float,
  d: Double,
  e: String
)
object PrimitiveTypeCombined {
  val primitiviesCodec: KafkaSerde[PrimitiveTypeCombined] =
    AvroFor[PrimitiveTypeCombined].asKey(sr).withTopic(TopicName("topic.avro"))

  val jsonPrimCodec: KafkaSerde[KJson[PrimitiveTypeCombined]] =
    AvroFor[KJson[PrimitiveTypeCombined]].asValue(sr).withTopic(TopicName("topic.json"))
}
