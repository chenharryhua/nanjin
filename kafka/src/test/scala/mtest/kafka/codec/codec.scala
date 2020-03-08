package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.codec.{KJson, NJCodec, SerdeOf}
import com.github.chenharryhua.nanjin.kafka.TopicName
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.circe.generic.auto._

package object codec extends ArbitraryData {

  val sr: Map[String, String] =
    Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://localhost:8081")

  val strCodec       = SerdeOf[String].asValue(sr).codec(TopicName("topic.str"))
  val intCodec       = SerdeOf[Int].asKey(sr).codec(TopicName("topic.int"))
  val longCodec      = SerdeOf[Long].asValue(sr).codec(TopicName("topic.long"))
  val doubleCodec    = SerdeOf[Double].asValue(sr).codec(TopicName("topic.double"))
  val floatCodec     = SerdeOf[Float].asKey(sr).codec(TopicName("topic.float"))
  val byteArrayCodec = SerdeOf[Array[Byte]].asKey(sr).codec(TopicName("topic.byte.array"))

  val primitiviesCodec =
    SerdeOf[PrimitiveTypeCombined].asKey(sr).codec(TopicName("topic.avro"))

  val jsonPrimCodec: NJCodec[KJson[PrimitiveTypeCombined]] =
    SerdeOf[KJson[PrimitiveTypeCombined]].asValue(sr).codec(TopicName("topic.json"))

}
