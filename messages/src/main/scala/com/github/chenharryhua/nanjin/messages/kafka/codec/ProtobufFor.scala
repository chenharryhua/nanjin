package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.google.protobuf.{Descriptors, DynamicMessage, Int32Value, Int64Value, StringValue}
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.util

sealed trait ProtobufFor[A] extends RegisterSerde[A] {
  def descriptor: Descriptors.Descriptor
  protected def serde: Serde[A]
}

object ProtobufFor {
  def apply[A](implicit ev: ProtobufFor[A]): ProtobufFor[A] = ev

  implicit val protobufForString: ProtobufFor[String] = new ProtobufFor[String] {
    override def descriptor: Descriptors.Descriptor = StringValue.getDescriptor
    override protected def serde: Serde[String] = serializable.stringSerde
  }

  implicit val protobufForLong: ProtobufFor[Long] = new ProtobufFor[Long] {
    override def descriptor: Descriptors.Descriptor = Int64Value.getDescriptor
    override protected def serde: Serde[Long] = serializable.longSerde
  }

  implicit val ProtobufForInt: ProtobufFor[Int] = new ProtobufFor[Int] {
    override def descriptor: Descriptors.Descriptor = Int32Value.getDescriptor
    override protected def serde: Serde[Int] = serializable.intSerde
  }

  implicit def protobufForGeneratedMessage[A <: GeneratedMessage](implicit
    gmc: GeneratedMessageCompanion[A]): ProtobufFor[A] =
    new ProtobufFor[A] {

      val descriptor: Descriptors.Descriptor = gmc.javaDescriptor

      override protected val serde: Serde[A] = new Serde[A] with Serializable {
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
