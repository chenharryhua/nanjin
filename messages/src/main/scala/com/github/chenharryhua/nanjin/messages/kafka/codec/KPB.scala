package com.github.chenharryhua.nanjin.messages.kafka.codec

import java.util

import com.google.protobuf.Message
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}

// kafka protobuf
final case class KPB[A](value: A)

object KPB {

  implicit def kpbSerializer[A <: Message]: KafkaAvroSerializer[KPB[A]] =
    new KafkaAvroSerializer[KPB[A]] {

      @transient private[this] lazy val ser: KafkaProtobufSerializer[A] =
        new KafkaProtobufSerializer[A]()

      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
        ser.configure(configs, isKey)

      override def close(): Unit = ser.close()

      override val avroEncoder: AvroEncoder[KPB[A]] = new AvroEncoder[KPB[A]] {

        override def encode(value: KPB[A]): Array[Byte] = value.value.toByteArray
        override val schemaFor: SchemaFor[KPB[A]]       = SchemaFor[Array[Byte]].forType[KPB[A]]
      }

      override def serialize(topic: String, data: KPB[A]): Array[Byte] =
        Option(data).flatMap(v => Option(v.value)) match {
          case None    => null.asInstanceOf[Array[Byte]]
          case Some(a) => ser.serialize(topic, a)
        }
    }

  implicit def kpbDeserializer[A <: Message]: KafkaAvroDeserializer[KPB[A]] =
    new KafkaAvroDeserializer[KPB[A]] {

      @transient private[this] lazy val deSer: KafkaProtobufDeserializer[A] =
        new KafkaProtobufDeserializer[A]()

      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
        deSer.configure(configs, isKey)

      override def close(): Unit = deSer.close()

      override val avroDecoder: AvroDecoder[KPB[A]] = new AvroDecoder[KPB[A]] {

        override val schemaFor: SchemaFor[KPB[A]] = SchemaFor[Array[Byte]].forType[KPB[A]]

        override def decode(value: Any): KPB[A] = sys.error("fix me")
      }

      override def deserialize(topic: String, data: Array[Byte]): KPB[A] =
        Option(data) match {
          case None    => null.asInstanceOf[KPB[A]]
          case Some(v) => KPB(deSer.deserialize(topic, v))
        }
    }
}
