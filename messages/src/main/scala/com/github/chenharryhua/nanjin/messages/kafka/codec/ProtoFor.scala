package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.google.protobuf.DynamicMessage
import com.google.protobuf.util.JsonFormat
import io.circe.Encoder as JsonEncoder
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.serialization.Serdes
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.util

sealed trait ProtoFor[A] extends RegisterSerde[A]

object ProtoFor {
  def apply[A](implicit ev: ProtoFor[A]): ProtoFor[A] = macro imp.summon[ProtoFor[A]]

  final class Universal(val value: DynamicMessage)
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

  implicit object protoForString extends ProtoFor[String] {
    override protected val unregisteredSerde: Serde[String] = Serdes.stringSerde
  }

  implicit object protoForLong extends ProtoFor[Long] {
    override protected val unregisteredSerde: Serde[Long] = Serdes.longSerde
  }

  implicit object protoForInt extends ProtoFor[Int] {
    override protected val unregisteredSerde: Serde[Int] = Serdes.intSerde
  }

  implicit object protoForUniversal extends ProtoFor[Universal] {
    override protected val unregisteredSerde: Serde[Universal] = new Serde[Universal] {
      override val serializer: Serializer[Universal] = new Serializer[Universal] {
        private[this] val ser = new KafkaProtobufSerializer[DynamicMessage]

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          ser.configure(configs, isKey)

        override def close(): Unit = ser.close()

        override def serialize(topic: String, data: Universal): Array[Byte] =
          Option(data).flatMap(u => Option(u.value)).map(dm => ser.serialize(topic, dm)).orNull
      }

      override val deserializer: Deserializer[Universal] = new Deserializer[Universal] {
        private[this] val deSer = new KafkaProtobufDeserializer[DynamicMessage]

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          deSer.configure(configs, isKey)

        override def close(): Unit = deSer.close()

        override def deserialize(topic: String, data: Array[Byte]): Universal =
          Option(deSer.deserialize(topic, data)).map(new Universal(_)).orNull
      }
    }
  }

  /*
   * General
   */

  implicit def protoForGeneratedMessage[A <: GeneratedMessage](implicit
    gmc: GeneratedMessageCompanion[A],
    ev: Null <:< A): ProtoFor[A] =
    new ProtoFor[A] {

      override protected val unregisteredSerde: Serde[A] = new Serde[A] {
        override val serializer: Serializer[A] = new Serializer[A] {

          private[this] val ser = new KafkaProtobufSerializer[DynamicMessage]

          override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
            ser.configure(configs, isKey)

          override def close(): Unit = ser.close()

          override def serialize(topic: String, data: A): Array[Byte] =
            Option(data).map { a =>
              val dm: DynamicMessage = DynamicMessage.parseFrom(a.companion.javaDescriptor, a.toByteArray)
              ser.serialize(topic, dm)
            }.orNull
        }

        override val deserializer: Deserializer[A] = new Deserializer[A] {
          private[this] val deSer = new KafkaProtobufDeserializer[DynamicMessage]

          override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
            deSer.configure(configs, isKey)

          override def close(): Unit = deSer.close()

          override def deserialize(topic: String, data: Array[Byte]): A =
            Option(deSer.deserialize(topic, data)).map(dm => gmc.parseFrom(dm.toByteArray)).orNull
        }
      }
    }
}
