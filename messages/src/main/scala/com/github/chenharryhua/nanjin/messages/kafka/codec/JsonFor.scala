package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.fasterxml.jackson.databind.JsonNode
import io.circe.jackson.{circeToJackson, jacksonToCirce}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import io.confluent.kafka.serializers.json.{KafkaJsonSchemaDeserializer, KafkaJsonSchemaSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import java.util

final class JsonFor[A] private (implicit EA: Encoder[A], DA: Decoder[A]) extends RegisterSerde[A] { outer =>
  @transient private lazy val serializer: Serializer[A] =
    new Serializer[A] with Serializable {
      private lazy val ser = new KafkaJsonSchemaSerializer[JsonNode]()

      override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
        ser.configure(configs, isKey)

      override def close(): Unit = ser.close()

      override def serialize(topic: String, data: A): Array[Byte] =
        if (data == null) null
        else
          ser.serialize(topic, circeToJackson(data.asJson))
    }

  @transient private lazy val deserializer: Deserializer[A] =
    new Deserializer[A] with Serializable {
      private lazy val deSer = new KafkaJsonSchemaDeserializer[JsonNode]()

      override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
        deSer.configure(configs, isKey)

      override def close(): Unit = deSer.close()

      override def deserialize(topic: String, data: Array[Byte]): A =
        if (data == null) null.asInstanceOf[A]
        else
          jacksonToCirce(deSer.deserialize(topic, data)).as[A] match {
            case Left(value)  => throw value
            case Right(value) => value
          }
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
  def apply[A: Encoder: Decoder]: JsonFor[A] = new JsonFor[A]
}
