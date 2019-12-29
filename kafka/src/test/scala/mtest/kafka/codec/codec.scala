package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.codec.{KafkaCodec, SerdeOf}
import com.github.chenharryhua.nanjin.kafka.codec.{KJson, KafkaCodec, SerdeOf}
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.circe.generic.auto._
package object codec extends ArbitraryData {

  val sr: Map[String, String] =
    Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://localhost:8081")

  val strCodec       = SerdeOf[String].asValue(sr).codec("topic.str")
  val intCodec       = SerdeOf[Int].asKey(sr).codec("topic.int")
  val longCodec      = SerdeOf[Long].asValue(sr).codec("topic.long")
  val doubleCodec    = SerdeOf[Double].asValue(sr).codec("topic.double")
  val floatCodec     = SerdeOf[Float].asKey(sr).codec("topic.float")
  val byteArrayCodec = SerdeOf[Array[Byte]].asKey(sr).codec("topic.byte.array")

  val primitiviesCodec =
    SerdeOf[PrimitiveTypeCombined].asKey(sr).codec("topic.avro")

  val jsonPrimCodec: KafkaCodec.Value[KJson[PrimitiveTypeCombined]] =
    SerdeOf[KJson[PrimitiveTypeCombined]].asValue(sr).codec("topic.json")

}
