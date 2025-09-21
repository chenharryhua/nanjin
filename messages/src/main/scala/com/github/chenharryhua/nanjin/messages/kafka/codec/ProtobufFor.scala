package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.google.protobuf.DynamicMessage
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.util

sealed trait ProtobufFor[A] extends RegisterSerde[A] {
  protected def unregisteredSerde: Serde[A]
}

object ProtobufFor {
  def apply[A](implicit ev: ProtobufFor[A]): ProtobufFor[A] = ev

  implicit object protobufForString extends ProtobufFor[String] {
    override protected val unregisteredSerde: Serde[String] = serializable.stringSerde
  }

  implicit object protobufForLong extends ProtobufFor[Long] {
    override protected val unregisteredSerde: Serde[Long] = serializable.longSerde
  }

  implicit object protobufForInt extends ProtobufFor[Int] {
    override protected val unregisteredSerde: Serde[Int] = serializable.intSerde
  }

  implicit object protobufForDynamicMessage extends ProtobufFor[DynamicMessage] {
    override protected def unregisteredSerde: Serde[DynamicMessage] = new Serde[DynamicMessage]
      with Serializable {
      override val serializer: Serializer[DynamicMessage] = new Serializer[DynamicMessage] with Serializable {
        @transient private[this] lazy val ser: KafkaProtobufSerializer[DynamicMessage] =
          new KafkaProtobufSerializer[DynamicMessage]

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          ser.configure(configs, isKey)

        override def close(): Unit = ser.close()

        override def serialize(topic: String, data: DynamicMessage): Array[Byte] = ser.serialize(topic, data)
      }
      override val deserializer: Deserializer[DynamicMessage] = new Deserializer[DynamicMessage]
        with Serializable {
        @transient private[this] lazy val deSer: KafkaProtobufDeserializer[DynamicMessage] =
          new KafkaProtobufDeserializer[DynamicMessage]

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          deSer.configure(configs, isKey)

        override def close(): Unit = deSer.close()

        override def deserialize(topic: String, data: Array[Byte]): DynamicMessage =
          deSer.deserialize(topic, data)
      }
    }
  }

  implicit def protobufForGeneratedMessage[A <: GeneratedMessage](implicit
    gmc: GeneratedMessageCompanion[A]): ProtobufFor[A] =
    new ProtobufFor[A] {

      override protected val unregisteredSerde: Serde[A] = new Serde[A] with Serializable {
        override val serializer: Serializer[A] =
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

        override val deserializer: Deserializer[A] =
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
      }
    }
}
