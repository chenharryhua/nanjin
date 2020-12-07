package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.google.protobuf.{CodedOutputStream, DynamicMessage}
import com.sksamuel.avro4s.{Codec, FieldMapper, SchemaFor}
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import scalapb.descriptors.{FieldDescriptor, PValue}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.util

// kafka protobuf
final class KPB[A <: GeneratedMessage] private (val value: A)
    extends GeneratedMessage with Serializable {
  override def writeTo(output: CodedOutputStream): Unit = value.writeTo(output)
  override def getFieldByNumber(fieldNumber: Int): Any  = value.getFieldByNumber(fieldNumber)
  override def getField(field: FieldDescriptor): PValue = value.getField(field)
  override def companion: GeneratedMessageCompanion[_]  = value.companion
  override def serializedSize: Int                      = value.serializedSize
  override def toProtoString: String                    = value.toProtoString
}

object KPB {
  def apply[A <: GeneratedMessage](a: A): KPB[A] = new KPB(a)

  implicit def kpbSchemaFor[A <: GeneratedMessage]: SchemaFor[KPB[A]] = new SchemaFor[KPB[A]] {
    override def schema: Schema           = SchemaFor[Array[Byte]].schema
    override def fieldMapper: FieldMapper = SchemaFor[Array[Byte]].fieldMapper
  }

  implicit def kpbCodec[A <: GeneratedMessage](implicit
    ev: GeneratedMessageCompanion[A]): Codec[KPB[A]] = new Codec[KPB[A]] {

    override def decode(value: Any): KPB[A] = value match {
      case ab: Array[Byte] => KPB(ev.parseFrom(ab))
      case ex              => sys.error(s"${ex.getClass} is not a Array[Byte] ${ex.toString}")
    }

    override def encode(value: KPB[A]): Array[Byte] = value.value.toByteArray
    override def schemaFor: SchemaFor[KPB[A]]       = kpbSchemaFor[A]
  }

  implicit def kpbSerde[A <: GeneratedMessage](implicit
    ev: GeneratedMessageCompanion[A]): SerdeOf[KPB[A]] =
    new SerdeOf[KPB[A]] {

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
              case Some(v) => KPB(ev.parseFrom(deSer.deserialize(topic, v).toByteArray))
            }
        }

      override val avroCodec: AvroCodec[KPB[A]] =
        AvroCodec[KPB[A]](kpbSchemaFor[A], kpbCodec[A], kpbCodec[A])

    }
}
