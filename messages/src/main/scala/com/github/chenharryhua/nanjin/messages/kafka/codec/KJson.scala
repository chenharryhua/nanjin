package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.{Eq, Show}
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import io.circe.parser
import io.circe.syntax._
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import org.apache.kafka.streams.scala.Serdes

final case class KJson[A](value: A)

object KJson {

  implicit def showKafkaJson[A: JsonEncoder]: Show[KJson[A]] =
    (t: KJson[A]) => s"""KJson(${Option(t.value).map(_.asJson.noSpaces).getOrElse("null")})"""
  implicit def eqKJson[A: Eq]: Eq[KJson[A]] = cats.derived.semi.eq[KJson[A]]

  implicit def jsonSerializer[A: JsonEncoder]: KafkaSerializer[KJson[A]] =
    new KafkaSerializer[KJson[A]] {

      override val avroEncoder: AvroEncoder[KJson[A]] = new AvroEncoder[KJson[A]] {

        override def encode(value: KJson[A]): String = value.value.asJson.noSpaces
        override val schemaFor: SchemaFor[KJson[A]]  = SchemaFor[String].forType[KJson[A]]
      }

      override def serialize(topic: String, data: KJson[A]): Array[Byte] = {
        val value: String = Option(data).flatMap(v => Option(v.value)) match {
          case None    => null.asInstanceOf[String]
          case Some(_) => avroEncoder.encode(data).asInstanceOf[String]
        }
        Serdes.String.serializer.serialize(topic, value)
      }
    }

  implicit def jsonDeserializer[A: JsonDecoder]: KafkaDeserializer[KJson[A]] =
    new KafkaDeserializer[KJson[A]] {

      override val avroDecoder: AvroDecoder[KJson[A]] = new AvroDecoder[KJson[A]] {

        override def decode(value: Any): KJson[A] =
          value match {
            case str: String =>
              parser.decode(str) match {
                case Right(r) => KJson(r)
                case Left(ex) => throw ex
              }
            case ex => sys.error(s"not a string")
          }

        override val schemaFor: SchemaFor[KJson[A]] = SchemaFor[String].forType[KJson[A]]
      }

      override def deserialize(topic: String, data: Array[Byte]): KJson[A] =
        Option(data) match {
          case None     => null.asInstanceOf[KJson[A]]
          case Some(ab) => avroDecoder.decode(Serdes.String.deserializer.deserialize(topic, ab))
        }
    }
}
