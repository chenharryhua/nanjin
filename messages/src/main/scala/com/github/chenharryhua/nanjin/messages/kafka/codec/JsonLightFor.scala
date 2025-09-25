package com.github.chenharryhua.nanjin.messages.kafka.codec

import io.circe.*
import io.circe.syntax.EncoderOps
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.util.UUID

/** JsonLightFor is schemaless json serde as opposite to JsonSchemaFor which has schema and talks to schema
  * register
  */
trait JsonLightFor[A] extends RegisterSerde[A]

object JsonLightFor {
  def apply[A](implicit ev: JsonLightFor[A]): JsonLightFor[A] = macro imp.summon[JsonLightFor[A]]

  final class Universal(val value: Json)
  object Universal {
    implicit val encoderUniversal: Encoder[Universal] = (a: Universal) => a.value
    implicit val decoderUniversal: Decoder[Universal] =
      (c: HCursor) => Right(new Universal(c.value))
  }

  /*
   * Specific
   */

  implicit object jsonLightForString extends JsonLightFor[String] {
    override protected val unregisteredSerde: Serde[String] = serializable.stringSerde
  }

  implicit object jsonLightForLong extends JsonLightFor[Long] {
    override protected val unregisteredSerde: Serde[Long] = serializable.longSerde
  }

  implicit object jsonLightForInt extends JsonLightFor[Int] {
    override protected val unregisteredSerde: Serde[Int] = serializable.intSerde
  }

  implicit object jsonLightForUUID extends JsonLightFor[UUID] {
    override protected val unregisteredSerde: Serde[UUID] = serializable.uuidSerde
  }

  implicit object jsonLightForUniversal extends JsonLightFor[Universal] {

    override protected val unregisteredSerde: Serde[Universal] =
      new Serde[Universal] with Serializable {
        override val serializer: Serializer[Universal] =
          new Serializer[Universal] with Serializable {
            @transient private[this] lazy val  ser = Serdes.byteBufferSerde.serializer()
            private val printer = Printer.noSpaces
            override def serialize(topic: String, data: Universal): Array[Byte] =
              Option(data)
                .flatMap(u => Option(u.value))
                .map(js => ser.serialize(topic, printer.printToByteBuffer(js)))
                .orNull
          }

        override val deserializer: Deserializer[Universal] =
          new Deserializer[Universal] with Serializable {
            override def deserialize(topic: String, data: Array[Byte]): Universal =
              Option(data).map { ab =>
                io.circe.jawn.parseByteArray(ab) match {
                  case Left(value)  => throw value
                  case Right(value) => new Universal(value)
                }
              }.orNull
          }
      }
  }

  /*
   * General
   */

  implicit def jsonLightForCodec[A: Encoder: Decoder](implicit ev: Null <:< A): JsonLightFor[A] =
    new JsonLightFor[A] {
      override protected val unregisteredSerde: Serde[A] =
        new Serde[A] with Serializable {
          override val serializer: Serializer[A] =
            new Serializer[A] with Serializable {
              @transient private[this] lazy val  ser = Serdes.byteBufferSerde.serializer()
              private val printer = Printer.noSpaces
              override def serialize(topic: String, data: A): Array[Byte] =
                Option(data).map(a => ser.serialize(topic, printer.printToByteBuffer(a.asJson))).orNull
            }

          override val deserializer: Deserializer[A] =
            new Deserializer[A] with Serializable {
              override def deserialize(topic: String, data: Array[Byte]): A =
                Option(data).map { ab =>
                  io.circe.jawn.decodeByteArray[A](ab) match {
                    case Left(value)  => throw value
                    case Right(value) => value
                  }
                }.orNull
            }
        }
    }
}
