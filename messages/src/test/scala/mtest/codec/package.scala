package mtest

import com.github.chenharryhua.nanjin.messages.kafka.codec.{KJson, NJCodec, SerdeOf}
import io.circe.generic.auto._

package object codec {
  val sr: Map[String, String] = Map("schema.registry.url" -> "http://localhost:8081")

  final case class PrimitiveTypeCombined(
    a: Int,
    b: Long,
    c: Float,
    d: Double,
    e: String
  )

  val strCodec: NJCodec[String]    = SerdeOf[String].asValue(sr).codec("topic.str")
  val intCodec: NJCodec[Int]       = SerdeOf[Int].asKey(sr).codec("topic.int")
  val longCodec: NJCodec[Long]     = SerdeOf[Long].asValue(sr).codec("topic.long")
  val doubleCodec: NJCodec[Double] = SerdeOf[Double].asValue(sr).codec("topic.double")
  val floatCodec: NJCodec[Float]   = SerdeOf[Float].asKey(sr).codec("topic.float")

  val byteArrayCodec: NJCodec[Array[Byte]] =
    SerdeOf[Array[Byte]].asKey(sr).codec("topic.byte.array")

  val primitiviesCodec: NJCodec[PrimitiveTypeCombined] =
    SerdeOf[PrimitiveTypeCombined].asKey(sr).codec("topic.avro")

  val jsonPrimCodec: NJCodec[KJson[PrimitiveTypeCombined]] =
    SerdeOf[KJson[PrimitiveTypeCombined]].asValue(sr).codec("topic.json")
}
