package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.{Distributive, Eq, Functor, Show}
import com.sksamuel.avro4s.{Codec, FieldMapper, SchemaFor}
import frameless.Injection
import io.circe.Decoder.Result
import io.circe.syntax.*
import io.circe.{jawn, Codec as JsonCodec, Decoder as JsonDecoder, Encoder as JsonEncoder, HCursor, Json}
import monocle.Iso
import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.streams.scala.serialization.Serdes

import scala.util.{Failure, Success, Try}

final class KJson[A] private (val value: A) extends Serializable {
  @SuppressWarnings(Array("IsInstanceOf"))
  def canEqual(a: Any): Boolean = a.isInstanceOf[KJson[?]]

  override def equals(that: Any): Boolean =
    that match {
      // equality is symmetric
      case that: KJson[?] => that.canEqual(this) && this.value == that.value
      case _              => false
    }
  override def hashCode: Int = value.hashCode()

  override def toString: String = s"KJson(value=${value.toString})"
}

object KJson {
  def apply[A](a: A): KJson[A] = new KJson[A](a)

  implicit def showKafkaJson[A: JsonEncoder]: Show[KJson[A]] =
    (t: KJson[A]) => s"""KJson(value=${Option(t.value).map(_.asJson.noSpaces).getOrElse("null")})"""

  implicit def eqKJson[A: Eq]: Eq[KJson[A]] = (x: KJson[A], y: KJson[A]) => Eq[A].eqv(x.value, y.value)

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
    override def encode(value: KJson[A]): String =
      Option(value).flatMap(v => Option(v.value)) match {
        case Some(value) => value.asJson.noSpaces
        case None        => null
      }

    override def decode(value: Any): KJson[A] = value match {
      case str: String =>
        jawn.decode[KJson[A]](str) match {
          case Right(r) => r
          case Left(ex) => throw new Exception(str, ex)
        }
      case utf8: Utf8 =>
        Try(utf8.toString) match {
          case Failure(exception) => throw exception
          case Success(str)       =>
            jawn.decode[A](str) match {
              case Right(r) => KJson(r)
              case Left(ex) => throw new Exception(str, ex)
            }
        }
      case null => null
      case ex   => sys.error(s"${ex.getClass.toString} is not a string: ${ex.toString}")
    }

    override def schemaFor: SchemaFor[KJson[A]] = SchemaFor[KJson[A]]
  }

  implicit def jsonSerde[A: JsonEncoder: JsonDecoder]: SerdeOf[KJson[A]] =
    new SerdeOf[KJson[A]] {

      override val avroCodec: AvroCodec[KJson[A]] = AvroCodec[KJson[A]]

      override val serializer: Serializer[KJson[A]] =
        new Serializer[KJson[A]] with Serializable {

          override def close(): Unit = ()

          @SuppressWarnings(Array("AsInstanceOf"))
          override def serialize(topic: String, data: KJson[A]): Array[Byte] =
            Serdes.stringSerde.serializer().serialize(topic, avroCodec.encode(data).asInstanceOf[String])
        }

      override val deserializer: Deserializer[KJson[A]] =
        new Deserializer[KJson[A]] with Serializable {

          override def close(): Unit = ()

          @SuppressWarnings(Array("AsInstanceOf"))
          override def deserialize(topic: String, data: Array[Byte]): KJson[A] =
            avroCodec.decode(Serdes.stringSerde.deserializer().deserialize(topic, data))
        }
    }

  implicit val distributiveKJson: Distributive[KJson] = new Distributive[KJson] {

    override def distribute[G[_], A, B](ga: G[A])(f: A => KJson[B])(implicit ev: Functor[G]): KJson[G[B]] = {
      val gb = ev.map(ga)(x => f(x).value)
      KJson(gb)
    }

    override def map[A, B](fa: KJson[A])(f: A => B): KJson[B] = KJson(f(fa.value))
  }

  implicit def isoKJson[A]: Iso[KJson[A], A] = Iso[KJson[A], A](_.value)(KJson(_))

  implicit def injectionKJson[A]: Injection[KJson[A], A] = new Injection[KJson[A], A] {
    override def apply(a: KJson[A]): A  = a.value
    override def invert(b: A): KJson[A] = KJson(b)
  }
}
