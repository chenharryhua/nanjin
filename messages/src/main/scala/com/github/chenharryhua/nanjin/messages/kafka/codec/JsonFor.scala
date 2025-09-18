package com.github.chenharryhua.nanjin.messages.kafka.codec

import io.confluent.kafka.serializers.json.{KafkaJsonSchemaDeserializer, KafkaJsonSchemaSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import java.util

final class JsonFor[A] private extends RegisterSerde[A] { outer =>
  @transient private lazy val serializer: Serializer[A] =
    new Serializer[A] with Serializable {
      private lazy val ser = new KafkaJsonSchemaSerializer[A]()

      override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
        ser.configure(configs, isKey)

      override def close(): Unit = ser.close()

      override def serialize(topic: String, data: A): Array[Byte] =
        ser.serialize(topic, data)
    }

  @transient private lazy val deserializer: Deserializer[A] =
    new Deserializer[A] with Serializable {
      private lazy val deSer = new KafkaJsonSchemaDeserializer[A]()

      override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
        deSer.configure(configs, isKey)

      override def close(): Unit = deSer.close()

      override def deserialize(topic: String, data: Array[Byte]): A =
        deSer.deserialize(topic, data)
    }

  override def asKey(props: Map[String, String]): Registered[A] =
    new Registered[A](
      new Serde[A] with Serializable {
        override def serializer: Serializer[A] = outer.serializer
        override def deserializer: Deserializer[A] = outer.deserializer
      },
      props,
      true
    )

  override def asValue(props: Map[String, String]): Registered[A] =
    new Registered(
      new Serde[A] with Serializable {
        override def serializer: Serializer[A] = outer.serializer
        override def deserializer: Deserializer[A] = outer.deserializer
      },
      props,
      false
    )
}
object JsonFor {
  def apply[A]: JsonFor[A] = new JsonFor[A]
}
