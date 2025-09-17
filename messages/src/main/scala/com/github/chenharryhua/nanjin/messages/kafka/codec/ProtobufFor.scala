package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.google.protobuf.DynamicMessage
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.util

final class ProtobufFor[A <: GeneratedMessage] private (gmc: GeneratedMessageCompanion[A])
    extends RegisterSerde[A] { outer =>
  private val serializer: Serializer[A] =
    new Serializer[A] with Serializable {

      @transient private[this] lazy val ser: KafkaProtobufSerializer[DynamicMessage] =
        new KafkaProtobufSerializer[DynamicMessage]

      override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
        ser.configure(configs, isKey)

      override def close(): Unit = ser.close()

      override def serialize(topic: String, data: A): Array[Byte] =
        if (data == null) null
        else {
          val dm = DynamicMessage.parseFrom(data.companion.javaDescriptor, data.toByteArray)
          ser.serialize(topic, dm)
        }
    }

  private val deserializer: Deserializer[A] =
    new Deserializer[A] with Serializable {

      @transient private[this] lazy val deSer: KafkaProtobufDeserializer[DynamicMessage] =
        new KafkaProtobufDeserializer[DynamicMessage]

      override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
        deSer.configure(configs, isKey)

      override def close(): Unit = deSer.close()

      override def deserialize(topic: String, data: Array[Byte]): A =
        if (data == null) null.asInstanceOf[A]
        else
          gmc.parseFrom(deSer.deserialize(topic, data).toByteArray)
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

object ProtobufFor {
  def apply[A <: GeneratedMessage](implicit gmc: GeneratedMessageCompanion[A]): ProtobufFor[A] =
    new ProtobufFor[A](gmc)
}
