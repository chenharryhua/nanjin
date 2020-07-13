package com.github.chenharryhua.nanjin.messages.kafka.codec

import java.util

import com.google.protobuf.DynamicMessage
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

// kafka protobuf
final case class KPB[A](value: A)

object KPB {

  implicit def kpbSerializer[A](implicit ev: A <:< GeneratedMessage): KafkaSerializer[KPB[A]] =
    new KafkaSerializer[KPB[A]] {

      @transient private[this] lazy val ser: KafkaProtobufSerializer[DynamicMessage] =
        new KafkaProtobufSerializer[DynamicMessage]()

      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
        ser.configure(configs, isKey)

      override def close(): Unit = ser.close()

      override val avroEncoder: AvroEncoder[KPB[A]] = new AvroEncoder[KPB[A]] {

        override def encode(value: KPB[A]): Array[Byte] = value.value.toByteArray
        override val schemaFor: SchemaFor[KPB[A]]       = SchemaFor[Array[Byte]].forType[KPB[A]]
      }

      override def serialize(topic: String, data: KPB[A]): Array[Byte] =
        Option(data).flatMap(v => Option(v.value)) match {
          case None => null.asInstanceOf[Array[Byte]]
          case Some(a) =>
            val dm = DynamicMessage.parseFrom(a.companion.javaDescriptor, a.toByteArray)
            ser.serialize(topic, dm)
        }
    }

  implicit def kpbDeserializer[A <: GeneratedMessage](implicit
    ev: GeneratedMessageCompanion[A]): KafkaDeserializer[KPB[A]] =
    new KafkaDeserializer[KPB[A]] {

      @transient private[this] lazy val deSer: KafkaProtobufDeserializer[DynamicMessage] =
        new KafkaProtobufDeserializer[DynamicMessage]()

      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
        deSer.configure(configs, isKey)

      override def close(): Unit = deSer.close()

      override val avroDecoder: AvroDecoder[KPB[A]] = new AvroDecoder[KPB[A]] {

        override val schemaFor: SchemaFor[KPB[A]] = SchemaFor[Array[Byte]].forType[KPB[A]]

        override def decode(value: Any): KPB[A] =
          value match {
            case ab: Array[Byte] => KPB(ev.parseFrom(ab))
            case ex              => sys.error(s"not a Array[Byte] ${ex.toString}")
          }
      }

      override def deserialize(topic: String, data: Array[Byte]): KPB[A] =
        Option(data) match {
          case None    => null.asInstanceOf[KPB[A]]
          case Some(v) => KPB(ev.parseFrom(deSer.deserialize(topic, data).toByteArray))
        }
    }
}
