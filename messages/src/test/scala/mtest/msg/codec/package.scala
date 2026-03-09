package mtest.msg

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroFor, JsonFor, KafkaSerde, ProtoFor}

package object codec {
  val sr: Map[String, String] = Map("schema.registry.url" -> "http://localhost:8081")

  val strCodec: KafkaSerde[String] = AvroFor[String].asValue(sr).withTopic(TopicName("topic.str"))
  val intCodec: KafkaSerde[java.lang.Integer] =
    AvroFor[java.lang.Integer].asKey(sr).withTopic(TopicName("topic.int"))
  val longCodec: KafkaSerde[java.lang.Long] =
    AvroFor[java.lang.Long].asValue(sr).withTopic(TopicName("topic.long"))
  val doubleCodec: KafkaSerde[java.lang.Double] =
    AvroFor[java.lang.Double].asValue(sr).withTopic(TopicName("topic.double"))
  val floatCodec: KafkaSerde[java.lang.Float] =
    AvroFor[java.lang.Float].asKey(sr).withTopic(TopicName("topic.float"))

  val byteArrayCodec: KafkaSerde[Array[Byte]] =
    AvroFor[Array[Byte]].asKey(sr).withTopic(TopicName("topic.byte.array"))

  val avro: KafkaSerde[CoproductJsons.Foo] =
    AvroFor[CoproductJsons.Foo].asValue(sr).withTopic(TopicName("avro.test"))
  val avroU: KafkaSerde[AvroFor.FromBroker] =
    AvroFor[AvroFor.FromBroker].asValue(sr).withTopic(TopicName("avro.test.universal"))

  val jsonSchema: KafkaSerde[CoproductJsons.Foo] =
    JsonFor[CoproductJsons.Foo].asValue(sr).withTopic(TopicName("json.schema.test"))

  val jsonSchemaU: KafkaSerde[JsonFor.FromBroker] =
    JsonFor[JsonFor.FromBroker].asValue(sr).withTopic(TopicName("json.schema.test.universal"))

  val protobufU: KafkaSerde[ProtoFor.FromBroker] =
    ProtoFor[ProtoFor.FromBroker].asValue(sr).withTopic(TopicName("protobuf.test.universal"))
}
