package mtest.msg

import com.github.chenharryhua.nanjin.messages.kafka.codec.{NJCodec, SerdeOf}

package object codec {
  val sr: Map[String, String] = Map("schema.registry.url" -> "http://localhost:8081")

  val strCodec: NJCodec[String]    = SerdeOf[String].asValue(sr).codec("topic.str")
  val intCodec: NJCodec[Int]       = SerdeOf[Int].asKey(sr).codec("topic.int")
  val longCodec: NJCodec[Long]     = SerdeOf[Long].asValue(sr).codec("topic.long")
  val doubleCodec: NJCodec[Double] = SerdeOf[Double].asValue(sr).codec("topic.double")
  val floatCodec: NJCodec[Float]   = SerdeOf[Float].asKey(sr).codec("topic.float")

  val byteArrayCodec: NJCodec[Array[Byte]] =
    SerdeOf[Array[Byte]].asKey(sr).codec("topic.byte.array")

}
