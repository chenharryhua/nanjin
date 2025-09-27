package mtest.msg

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroFor, JsonFor, KafkaSerde, ProtoFor}
import eu.timepit.refined.auto.*

package object codec {
  val sr: Map[String, String] = Map("schema.registry.url" -> "http://localhost:8081")

  val strCodec: KafkaSerde[String] = AvroFor[String].asValue(sr).withTopic(TopicName("topic.str"))
  val intCodec: KafkaSerde[Int] = AvroFor[Int].asKey(sr).withTopic(TopicName("topic.int"))
  val longCodec: KafkaSerde[Long] = AvroFor[Long].asValue(sr).withTopic(TopicName("topic.long"))
  val doubleCodec: KafkaSerde[Double] = AvroFor[Double].asValue(sr).withTopic(TopicName("topic.double"))
  val floatCodec: KafkaSerde[Float] = AvroFor[Float].asKey(sr).withTopic(TopicName("topic.float"))

  val byteArrayCodec: KafkaSerde[Array[Byte]] =
    AvroFor[Array[Byte]].asKey(sr).withTopic(TopicName("topic.byte.array"))

  val avro: KafkaSerde[CoproductJsons.Foo] =
    AvroFor[CoproductJsons.Foo].asValue(sr).withTopic(TopicName("avro.test"))
  val avroU: KafkaSerde[AvroFor.Universal] =
    AvroFor[AvroFor.Universal].asValue(sr).withTopic(TopicName("avro.test.universal"))

  val jsonSchema: KafkaSerde[CoproductJsons.Foo] =
    JsonFor[CoproductJsons.Foo].asValue(sr).withTopic(TopicName("json.schema.test"))

  val jsonSchemaU: KafkaSerde[JsonFor.Universal] =
    JsonFor[JsonFor.Universal].asValue(sr).withTopic(TopicName("json.schema.test.universal"))

  val protobufU: KafkaSerde[ProtoFor.Universal] =
    ProtoFor[ProtoFor.Universal].asValue(sr).withTopic(TopicName("protobuf.test.universal"))
}
