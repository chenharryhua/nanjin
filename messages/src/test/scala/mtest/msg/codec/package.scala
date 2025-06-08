package mtest.msg

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodecOf, KafkaSerde}
import eu.timepit.refined.auto.*

package object codec {
  val sr: Map[String, String] = Map("schema.registry.url" -> "http://localhost:8081")

  val strCodec: KafkaSerde[String] = AvroCodecOf[String].asValue(sr).withTopic(TopicName("topic.str"))
  val intCodec: KafkaSerde[Int] = AvroCodecOf[Int].asKey(sr).withTopic(TopicName("topic.int"))
  val longCodec: KafkaSerde[Long] = AvroCodecOf[Long].asValue(sr).withTopic(TopicName("topic.long"))
  val doubleCodec: KafkaSerde[Double] = AvroCodecOf[Double].asValue(sr).withTopic(TopicName("topic.double"))
  val floatCodec: KafkaSerde[Float] = AvroCodecOf[Float].asKey(sr).withTopic(TopicName("topic.float"))

  val byteArrayCodec: KafkaSerde[Array[Byte]] =
    AvroCodecOf[Array[Byte]].asKey(sr).withTopic(TopicName("topic.byte.array"))

}
