package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.google.protobuf.DynamicMessage
import com.google.protobuf.util.JsonFormat
import io.circe.Encoder as JsonEncoder
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import io.estatico.newtype.macros.newtype
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.util

sealed trait ProtobufFor[A] extends RegisterSerde[A]

object ProtobufFor {
  def apply[A](implicit ev: ProtobufFor[A]): ProtobufFor[A] = macro imp.summon[ProtobufFor[A]]

  @newtype final case class Universal(value: DynamicMessage)
  object Universal {
    private val jsonFormat = JsonFormat.printer()
    implicit val jsonEncoderUniversal: JsonEncoder[Universal] =
      (a: Universal) =>
        io.circe.jawn.parse(jsonFormat.print(a.value)) match {
          case Left(value)  => throw value
          case Right(value) => value
        }
  }

  /*
   * Specific
   */

  implicit object protobufForString extends ProtobufFor[String] {
    override protected val unregisteredSerde: Serde[String] = serializable.stringSerde
  }

  implicit object protobufForLong extends ProtobufFor[Long] {
    override protected val unregisteredSerde: Serde[Long] = serializable.longSerde
  }

  implicit object protobufForInt extends ProtobufFor[Int] {
    override protected val unregisteredSerde: Serde[Int] = serializable.intSerde
  }

  implicit object protobufForUniversal extends ProtobufFor[Universal] {
    override protected val unregisteredSerde: Serde[Universal] = new Serde[Universal] with Serializable {
      override val serializer: Serializer[Universal] = new Serializer[Universal] with Serializable {
        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit = ()
        override def close(): Unit = ()
        override def serialize(topic: String, data: Universal): Array[Byte] =
          throw ForbiddenProduceException("Protobuf")
      }
      override val deserializer: Deserializer[Universal] = new Deserializer[Universal] with Serializable {
        @transient private[this] lazy val deSer = new KafkaProtobufDeserializer[DynamicMessage]

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          deSer.configure(configs, isKey)

        override def close(): Unit = deSer.close()

        override def deserialize(topic: String, data: Array[Byte]): Universal =
          Universal(deSer.deserialize(topic, data))
      }
    }
  }

  /*
   * General
   */

  implicit def protobufForGeneratedMessage[A <: GeneratedMessage](implicit
    gmc: GeneratedMessageCompanion[A]): ProtobufFor[A] =
    new ProtobufFor[A] {

      override protected val unregisteredSerde: Serde[A] = new Serde[A] with Serializable {
        override val serializer: Serializer[A] =
          new Serializer[A] with Serializable {

            @transient private[this] lazy val ser = new KafkaProtobufSerializer[DynamicMessage]

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

            @transient private[this] lazy val deSer = new KafkaProtobufDeserializer[DynamicMessage]

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
