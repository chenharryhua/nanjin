package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.Show
import cats.implicits.showInterpolator
import cats.kernel.Eq
import com.google.protobuf.{CodedInputStream, CodedOutputStream, Descriptors, DynamicMessage}
import com.sksamuel.avro4s.{Codec, SchemaFor}
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import scalapb.descriptors.{Descriptor, FieldDescriptor, PValue, Reads}
import scalapb.{GeneratedEnumCompanion, GeneratedMessage, GeneratedMessageCompanion}

import java.util
import scala.annotation.nowarn

// kafka protobuf
final class KPB[A <: GeneratedMessage] private (val value: A) extends GeneratedMessage with Serializable {
  // equality
  @SuppressWarnings(Array("IsInstanceOf"))
  def canEqual(a: Any): Boolean = a.isInstanceOf[KPB[?]]

  override def equals(that: Any): Boolean =
    that match {
      case that: KPB[?] => that.canEqual(this) && this.value == that.value
      case _            => false
    }
  override def hashCode: Int = value.hashCode()

  // override GeneratedMessage
  override def writeTo(output: CodedOutputStream): Unit = value.writeTo(output)
  @nowarn
  override def getFieldByNumber(fieldNumber: Int): Any = value.getFieldByNumber(fieldNumber)
  override def getField(field: FieldDescriptor): PValue = value.getField(field)
  override def companion: GeneratedMessageCompanion[?] = value.companion
  override def serializedSize: Int = value.serializedSize
  override def toProtoString: String = value.toProtoString
  override def productElement(n: Int): Any = value.productElement(n)
  override def productArity: Int = value.productArity
}

object KPB {
  def apply[A <: GeneratedMessage](a: A): KPB[A] = new KPB(a)

  implicit def eqKPB[A <: GeneratedMessage: Eq]: Eq[KPB[A]] = (x: KPB[A], y: KPB[A]) =>
    Eq[A].eqv(x.value, y.value)

  implicit def showKPB[A <: GeneratedMessage: Show]: Show[KPB[A]] =
    (t: KPB[A]) => show"KPB(value=${t.value})"

  implicit def kbpCompanion[A <: GeneratedMessage](implicit
    ev: GeneratedMessageCompanion[A]): GeneratedMessageCompanion[KPB[A]] =
    new GeneratedMessageCompanion[KPB[A]] with Serializable {

      override def merge(a: KPB[A], input: CodedInputStream): KPB[A] = KPB(ev.merge(a.value, input))
      override def javaDescriptor: Descriptors.Descriptor = ev.javaDescriptor
      override def scalaDescriptor: Descriptor = ev.scalaDescriptor

      override def nestedMessagesCompanions: Seq[GeneratedMessageCompanion[? <: GeneratedMessage]] =
        ev.nestedMessagesCompanions
      override def messageReads: Reads[KPB[A]] = Reads(pv => KPB(ev.messageReads.read(pv)))

      override def messageCompanionForFieldNumber(field: Int): GeneratedMessageCompanion[?] =
        ev.messageCompanionForFieldNumber(field)

      override def enumCompanionForFieldNumber(field: Int): GeneratedEnumCompanion[?] =
        ev.enumCompanionForFieldNumber(field)
      override def defaultInstance: KPB[A] = KPB(ev.defaultInstance)

      override def parseFrom(input: CodedInputStream): KPB[A] = KPB(ev.parseFrom(input))
    }

  implicit def kpbAvroCodec[A <: GeneratedMessage](implicit
    ev: GeneratedMessageCompanion[A]): AvroCodecOf[KPB[A]] =
    new AvroCodecOf[KPB[A]] {
      override val avroCodec: AvroCodec[KPB[A]] = {
        val kpbCodec: Codec[KPB[A]] = new Codec[KPB[A]] {
          override def decode(value: Any): KPB[A] = value match {
            case ab: Array[Byte] => KPB(ev.parseFrom(ab))
            case null            => null
            case ex              => sys.error(s"${ex.getClass.toString} is not Array[Byte]")
          }

          override def encode(value: KPB[A]): Array[Byte] =
            if (value == null) null else value.value.toByteArray

          override def schemaFor: SchemaFor[KPB[A]] = SchemaFor[Array[Byte]].forType[KPB[A]]
        }
        AvroCodec[KPB[A]](kpbCodec.schemaFor, kpbCodec, kpbCodec)
      }

      override val serializer: Serializer[KPB[A]] =
        new Serializer[KPB[A]] with Serializable {

          @transient private[this] lazy val ser: KafkaProtobufSerializer[DynamicMessage] =
            new KafkaProtobufSerializer[DynamicMessage]

          override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
            ser.configure(configs, isKey)

          override def close(): Unit =
            ser.close()

          @SuppressWarnings(Array("AsInstanceOf"))
          override def serialize(topic: String, data: KPB[A]): Array[Byte] =
            Option(data).flatMap(v => Option(v.value)) match {
              case None    => null.asInstanceOf[Array[Byte]]
              case Some(a) =>
                val dm = DynamicMessage.parseFrom(a.companion.javaDescriptor, a.toByteArray)
                ser.serialize(topic, dm)
            }
        }

      override val deserializer: Deserializer[KPB[A]] =
        new Deserializer[KPB[A]] with Serializable {

          @transient private[this] lazy val deSer: KafkaProtobufDeserializer[DynamicMessage] =
            new KafkaProtobufDeserializer[DynamicMessage]

          override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
            deSer.configure(configs, isKey)

          override def close(): Unit =
            deSer.close()

          @SuppressWarnings(Array("AsInstanceOf"))
          override def deserialize(topic: String, data: Array[Byte]): KPB[A] =
            Option(data) match {
              case None    => null.asInstanceOf[KPB[A]]
              case Some(v) => KPB(ev.parseFrom(deSer.deserialize(topic, v).toByteArray))
            }
        }
    }
}
