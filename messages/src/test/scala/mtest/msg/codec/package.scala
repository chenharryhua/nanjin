package mtest.msg

import com.github.chenharryhua.nanjin.messages.kafka.codec.{KafkaSerde, SerdeOf}

package object codec {
  val sr: Map[String, String] = Map("schema.registry.url" -> "http://localhost:8081")

  val strCodec: KafkaSerde[String]    = SerdeOf[String].asValue(sr).topic("topic.str")
  val intCodec: KafkaSerde[Int]       = SerdeOf[Int].asKey(sr).topic("topic.int")
  val longCodec: KafkaSerde[Long]     = SerdeOf[Long].asValue(sr).topic("topic.long")
  val doubleCodec: KafkaSerde[Double] = SerdeOf[Double].asValue(sr).topic("topic.double")
  val floatCodec: KafkaSerde[Float]   = SerdeOf[Float].asKey(sr).topic("topic.float")

  val byteArrayCodec: KafkaSerde[Array[Byte]] =
    SerdeOf[Array[Byte]].asKey(sr).topic("topic.byte.array")

}
