package com.github.chenharryhua.nanjin.messages.kafka.codec

import java.util

import com.google.protobuf.DynamicMessage
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

// kafka protobuf
final case class KPB[A](value: A)

object KPB {

  implicit def kpbSerde[A <: GeneratedMessage](implicit
    ev: GeneratedMessageCompanion[A]): SerdeOf[KPB[A]] =
    new SerdeOf[KPB[A]] {
      val schemaForPB: SchemaFor[KPB[A]] = SchemaFor[Array[Byte]].forType[KPB[A]]

      val avroEncoder: AvroEncoder[KPB[A]] = new AvroEncoder[KPB[A]] {
        override def encode(value: KPB[A]): Array[Byte] = value.value.toByteArray
        override val schemaFor: SchemaFor[KPB[A]]       = schemaForPB
      }

      val avroDecoder: AvroDecoder[KPB[A]] = new AvroDecoder[KPB[A]] {
        override val schemaFor: SchemaFor[KPB[A]] = schemaForPB

        override def decode(value: Any): KPB[A] =
          value match {
            case ab: Array[Byte] => KPB(ev.parseFrom(ab))
            case ex              => sys.error(s"${ex.getClass} is not a Array[Byte] ${ex.toString}")
          }
      }

      override val serializer: Serializer[KPB[A]] =
        new Serializer[KPB[A]] with Serializable {

          @transient private[this] lazy val ser: KafkaProtobufSerializer[DynamicMessage] =
            new KafkaProtobufSerializer[DynamicMessage]()

          override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
            ser.configure(configs, isKey)

          override def close(): Unit =
            ser.close()

          override def serialize(topic: String, data: KPB[A]): Array[Byte] =
            Option(data).flatMap(v => Option(v.value)) match {
              case None => null.asInstanceOf[Array[Byte]]
              case Some(a) =>
                val dm = DynamicMessage.parseFrom(a.companion.javaDescriptor, a.toByteArray)
                ser.serialize(topic, dm)
            }

        }

      override val deserializer: Deserializer[KPB[A]] =
        new Deserializer[KPB[A]] with Serializable {

          @transient private[this] lazy val deSer: KafkaProtobufDeserializer[DynamicMessage] =
            new KafkaProtobufDeserializer[DynamicMessage]()

          override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
            deSer.configure(configs, isKey)

          override def close(): Unit =
            deSer.close()

          override def deserialize(topic: String, data: Array[Byte]): KPB[A] =
            Option(data) match {
              case None    => null.asInstanceOf[KPB[A]]
              case Some(v) => KPB(ev.parseFrom(deSer.deserialize(topic, data).toByteArray))
            }
        }

      override val avroCodec: AvroCodec[KPB[A]] =
        AvroCodec[KPB[A]](schemaForPB, avroDecoder, avroEncoder)

    }
}
