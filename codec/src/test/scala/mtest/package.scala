import com.github.chenharryhua.nanjin.codec.{KJson, SerdeOf}
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import monocle.Prism

package object mtest {

  val sr: Map[String, String] =
    Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://localhost:8081")

  final case class PrimitiveTypeCombined(
    a: Int,
    b: Long,
    c: Float,
    d: Double,
    e: String
  )

  val strCodec       = SerdeOf[String].asValue(sr).codec("topic.str")
  val intCodec       = SerdeOf[Int].asKey(sr).codec("topic.int")
  val longCodec      = SerdeOf[Long].asValue(sr).codec("topic.long")
  val doubleCodec    = SerdeOf[Double].asValue(sr).codec("topic.double")
  val floatCodec     = SerdeOf[Float].asKey(sr).codec("topic.float")
  val byteArrayCodec = SerdeOf[Array[Byte]].asKey(sr).codec("topic.byte.array")

  val primitiviesCodec =
    SerdeOf[PrimitiveTypeCombined].asKey(sr).codec(s"prism.test.topic.avro")

  val jsonPrimCodec =
    SerdeOf[KJson[PrimitiveTypeCombined]].asKey(sr).codec(s"prism.test.topic.json")

}
