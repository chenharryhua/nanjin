package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.implicits.catsSyntaxOptionId
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.messages.ProtoPrimitive.{ProtoInt, ProtoLong, ProtoString}
import com.google.protobuf.DynamicMessage
import com.google.protobuf.util.JsonFormat
import io.circe.{Encoder as JsonEncoder, Json}
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.util

sealed trait ProtoFor[A] extends UnregisteredSerde[A] {
  def protobufSchema: Option[ProtobufSchema]
}

object ProtoFor {
  def apply[A](implicit ev: ProtoFor[A]): ProtoFor[A] = ev

  opaque type FromBroker = DynamicMessage
  object FromBroker:
    private[ProtoFor] def apply(dm: DynamicMessage): FromBroker = dm
    extension (dm: FromBroker) def value: DynamicMessage = dm
    given JsonEncoder[FromBroker] with
      private val jsonFormat = JsonFormat.printer()
      override def apply(a: FromBroker): Json =
        io.circe.jawn.parse(jsonFormat.print(a.value)) match {
          case Left(value)  => throw value // scalafix:ok
          case Right(value) => value
        }
    given Eq[FromBroker] = Eq.fromUniversalEquals

  /*
   * Specific
   */

  implicit object protoForString extends ProtoFor[String] {
    override val isPrimitive: Boolean = true
    override protected val unregisteredSerde: Serde[String] = Serdes.String()
    override val protobufSchema: Option[ProtobufSchema] = new ProtobufSchema(ProtoString.javaDescriptor).some
  }

  implicit object protoForLong extends ProtoFor[java.lang.Long] {
    override val isPrimitive: Boolean = true
    override protected val unregisteredSerde: Serde[java.lang.Long] = Serdes.Long()
    override val protobufSchema: Option[ProtobufSchema] = new ProtobufSchema(ProtoLong.javaDescriptor).some
  }

  implicit object protoForInt extends ProtoFor[java.lang.Integer] {
    override val isPrimitive: Boolean = true
    override protected val unregisteredSerde: Serde[java.lang.Integer] = Serdes.Integer()
    override val protobufSchema: Option[ProtobufSchema] = new ProtobufSchema(ProtoInt.javaDescriptor).some
  }

  implicit object protoForFromBroker extends ProtoFor[FromBroker] {
    override val isPrimitive: Boolean = false
    override val protobufSchema: Option[ProtobufSchema] = None

    override protected val unregisteredSerde: Serde[FromBroker] = new Serde[FromBroker] {
      override val serializer: Serializer[FromBroker] = new Serializer[FromBroker] {
        private val ser = new KafkaProtobufSerializer[DynamicMessage]

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          ser.configure(configs, isKey)

        override def close(): Unit = ser.close()

        override def serialize(topic: String, data: FromBroker): Array[Byte] =
          Option(data).map(dm => ser.serialize(topic, dm)).orNull
      }

      override val deserializer: Deserializer[FromBroker] = new Deserializer[FromBroker] {
        private val deSer = new KafkaProtobufDeserializer[DynamicMessage]

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          deSer.configure(configs, isKey)

        override def close(): Unit = deSer.close()

        override def deserialize(topic: String, data: Array[Byte]): FromBroker =
          Option(deSer.deserialize(topic, data))
            .map(FromBroker(_))
            .orNull

      }
    }
  }

  /*
   * General
   */

  implicit def protoForGeneratedMessage[A <: GeneratedMessage](implicit
    gmc: GeneratedMessageCompanion[A],
    ev: Null <:< A): ProtoFor[A] = new ProtoFor[A] {
    override val isPrimitive: Boolean = false

    override val protobufSchema: Option[ProtobufSchema] = new ProtobufSchema(gmc.javaDescriptor).some

    override protected val unregisteredSerde: Serde[A] = new Serde[A] {
      override val serializer: Serializer[A] = new Serializer[A] {

        private val ser = new KafkaProtobufSerializer[DynamicMessage]

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
        private val deSer = new KafkaProtobufDeserializer[DynamicMessage]

        override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
          deSer.configure(configs, isKey)

        override def close(): Unit = deSer.close()

        override def deserialize(topic: String, data: Array[Byte]): A =
          Option(deSer.deserialize(topic, data))
            .map(dm => gmc.parseFrom(dm.toByteArray))
            .orNull

      }
    }
  }
}
