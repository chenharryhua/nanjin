package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.{Eq, Show}
import com.sksamuel.avro4s.{Codec, FieldMapper, SchemaFor}
import io.circe.Decoder.Result
import io.circe.syntax._
import io.circe.{
  HCursor,
  Json,
  parser,
  Codec => JsonCodec,
  Decoder => JsonDecoder,
  Encoder => JsonEncoder
}
import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.streams.scala.Serdes

final class KJson[A] private (val value: A) extends Serializable {
  def canEqual(a: Any): Boolean = a.isInstanceOf[KJson[A]]

  override def equals(that: Any): Boolean =
    that match {
      // equality is symmetric
      case that: KJson[A] => that.canEqual(this) && this.value == that.value
      case _              => false
    }
  override def hashCode: Int = value.hashCode()
}

object KJson {
  def apply[A](a: A): KJson[A] = new KJson[A](a)

  implicit def showKafkaJson[A: JsonEncoder]: Show[KJson[A]] =
    (t: KJson[A]) => s"""KJson(${Option(t.value).map(_.asJson.noSpaces).getOrElse("null")})"""

  implicit def eqKJson[A: Eq]: Eq[KJson[A]] = (x: KJson[A], y: KJson[A]) =>
    Eq[A].eqv(x.value, y.value)

  implicit def kjsonJsonCodec[A: JsonEncoder: JsonDecoder]: JsonCodec[KJson[A]] =
    new JsonCodec[KJson[A]] {
      override def apply(a: KJson[A]): Json            = JsonEncoder[A].apply(a.value)
      override def apply(c: HCursor): Result[KJson[A]] = JsonDecoder[A].apply(c).map(KJson[A])
    }

  implicit def avroKJsonSchemaFor[A]: SchemaFor[KJson[A]] = new SchemaFor[KJson[A]] {
    override def schema: Schema           = SchemaFor[String].schema
    override def fieldMapper: FieldMapper = SchemaFor[String].fieldMapper
  }

  implicit def avroKJsonCodec[A: JsonEncoder: JsonDecoder]: Codec[KJson[A]] = new Codec[KJson[A]] {
    override def encode(value: KJson[A]): String = value.value.asJson.noSpaces

    override def decode(value: Any): KJson[A] = value match {
      case str: String =>
        parser.decode[KJson[A]](str) match {
          case Right(r) => r
          case Left(ex) => throw ex
        }
      case utf8: Utf8 =>
        parser.decode[KJson[A]](utf8.toString) match {
          case Right(r) => r
          case Left(ex) => throw ex
        }
      case ex => sys.error(s"${ex.getClass} is not a string: $ex")
    }

    override def schemaFor: SchemaFor[KJson[A]] = SchemaFor[KJson[A]]
  }

  implicit def jsonSerde[A: JsonEncoder: JsonDecoder]: SerdeOf[KJson[A]] =
    new SerdeOf[KJson[A]] {
      private val cachedCodec: Codec[KJson[A]] = avroKJsonCodec[A]

      override val avroCodec: AvroCodec[KJson[A]] =
        AvroCodec(cachedCodec.schemaFor, cachedCodec, cachedCodec)

      override val serializer: Serializer[KJson[A]] =
        new Serializer[KJson[A]] with Serializable {
          override def close(): Unit = ()

          override def serialize(topic: String, data: KJson[A]): Array[Byte] = {
            val value: String = Option(data).flatMap(v => Option(v.value)) match {
              case None    => null.asInstanceOf[String]
              case Some(_) => avroCodec.avroEncoder.encode(data).asInstanceOf[String]
            }
            Serdes.String.serializer.serialize(topic, value)
          }
        }

      override val deserializer: Deserializer[KJson[A]] =
        new Deserializer[KJson[A]] with Serializable {
          override def close(): Unit = ()

          override def deserialize(topic: String, data: Array[Byte]): KJson[A] =
            Option(data) match {
              case None => null.asInstanceOf[KJson[A]]
              case Some(ab) =>
                avroCodec.avroDecoder.decode(Serdes.String.deserializer.deserialize(topic, ab))
            }
        }

    }
}
