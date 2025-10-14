package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.implicits.catsSyntaxOptionId
import com.github.chenharryhua.nanjin.messages.ProtoPrimitive.{ProtoInt, ProtoLong, ProtoString}
import com.google.protobuf.DynamicMessage
import com.google.protobuf.util.JsonFormat
import io.circe.Encoder as JsonEncoder
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import io.estatico.newtype.macros.newtype
import io.estatico.newtype.ops.toCoercibleIdOps
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.serialization.Serdes
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.util

sealed trait ProtoFor[A] extends RegisterSerde[A] {
  def protobufSchema: Option[ProtobufSchema]
}

object ProtoFor {
  def apply[A](implicit ev: ProtoFor[A]): ProtoFor[A] = ev

  @newtype final class FromBroker private (val value: DynamicMessage)
  object FromBroker {
    def apply(dm: DynamicMessage): FromBroker = dm.coerce
    private val jsonFormat = JsonFormat.printer()
    implicit val jsonEncoderUniversal: JsonEncoder[FromBroker] =
      (a: FromBroker) =>
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
    override val protobufSchema: Option[ProtobufSchema] = new ProtobufSchema(ProtoString.javaDescriptor).some
  }

  implicit object protoForLong extends ProtoFor[Long] {
    override protected val unregisteredSerde: Serde[Long] = Serdes.longSerde
    override val protobufSchema: Option[ProtobufSchema] = new ProtobufSchema(ProtoLong.javaDescriptor).some
  }

  implicit object protoForInt extends ProtoFor[Int] {
    override protected val unregisteredSerde: Serde[Int] = Serdes.intSerde
    override val protobufSchema: Option[ProtobufSchema] = new ProtobufSchema(ProtoInt.javaDescriptor).some
  }

  implicit object protoForFromBroker extends ProtoFor[FromBroker] {
    override val protobufSchema: Option[ProtobufSchema] = None

    override protected val unregisteredSerde: Serde[FromBroker] = new Serde[FromBroker] {
      override val serializer: Serializer[FromBroker] = new Serializer[FromBroker] {
        private[this] val ser = new KafkaProtobufSerializer[DynamicMessage]

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          ser.configure(configs, isKey)

        override def close(): Unit = ser.close()

        override def serialize(topic: String, data: FromBroker): Array[Byte] =
          Option(data).flatMap(u => Option(u.value)).map(dm => ser.serialize(topic, dm)).orNull
      }

      override val deserializer: Deserializer[FromBroker] = new Deserializer[FromBroker] {
        private[this] val deSer = new KafkaProtobufDeserializer[DynamicMessage]

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          deSer.configure(configs, isKey)

        override def close(): Unit = deSer.close()

        override def deserialize(topic: String, data: Array[Byte]): FromBroker =
          Option(deSer.deserialize(topic, data))
            .map(_.coerce[FromBroker])
            .getOrElse(null.asInstanceOf[FromBroker])
      }
    }
  }

  /*
   * General
   */

  implicit def protoForGeneratedMessage[A <: GeneratedMessage](implicit
    gmc: GeneratedMessageCompanion[A]): ProtoFor[A] = new ProtoFor[A] {
    override val protobufSchema: Option[ProtobufSchema] = new ProtobufSchema(gmc.javaDescriptor).some

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
          Option(deSer.deserialize(topic, data))
            .map(dm => gmc.parseFrom(dm.toByteArray))
            .getOrElse(null.asInstanceOf[A])
      }
    }
  }
}
