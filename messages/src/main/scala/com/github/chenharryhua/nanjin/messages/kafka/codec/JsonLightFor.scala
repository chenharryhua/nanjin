package com.github.chenharryhua.nanjin.messages.kafka.codec

import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json, Printer}
import io.estatico.newtype.macros.newtype
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.nio.ByteBuffer
import java.util.UUID

/** JsonLightFor is schemaless json serde as opposite to JsonSchemaFor which has schema and talks to schema
  * register
  */
trait JsonLightFor[A] extends RegisterSerde[A]

object JsonLightFor {
  def apply[A](implicit ev: JsonLightFor[A]): JsonLightFor[A] = macro imp.summon[JsonLightFor[A]]

  @newtype final case class Universal(value: Json)
  object Universal {
    implicit val encoderUniversal: Encoder[Universal] = (a: Universal) => a.value
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

  private val serdes_internal: Serde[ByteBuffer] = Serdes.byteBufferSerde

  implicit object jsonLightForUniversal extends JsonLightFor[Universal] {

    override protected val unregisteredSerde: Serde[Universal] =
      new Serde[Universal] with Serializable {
        override val serializer: Serializer[Universal] =
          new Serializer[Universal] with Serializable {
            override def serialize(topic: String, data: Universal): Array[Byte] =
              throw ForbiddenProduceException("JsonLight")
          }

        override val deserializer: Deserializer[Universal] =
          new Deserializer[Universal] with Serializable {
            private val deSer = serdes_internal.deserializer()
            override def deserialize(topic: String, data: Array[Byte]): Universal =
              if (data == null) null.asInstanceOf[Universal]
              else {
                io.circe.jawn.parseByteBuffer(deSer.deserialize(topic, data)) match {
                  case Left(value)  => throw value
                  case Right(value) => Universal(value)
                }
              }
          }
      }
  }

  /*
   * General
   */

  implicit def jsonLightForCodec[A: Encoder: Decoder]: JsonLightFor[A] =
    new JsonLightFor[A] {
      override protected val unregisteredSerde: Serde[A] =
        new Serde[A] with Serializable {
          override val serializer: Serializer[A] =
            new Serializer[A] with Serializable {
              private val ser = serdes_internal.serializer()
              private val print = Printer.noSpaces
              override def serialize(topic: String, data: A): Array[Byte] =
                if (data == null) null
                else ser.serialize(topic, print.printToByteBuffer(data.asJson))
            }

          override val deserializer: Deserializer[A] =
            new Deserializer[A] with Serializable {
              private val deSer = serdes_internal.deserializer()
              override def deserialize(topic: String, data: Array[Byte]): A =
                if (data == null) null.asInstanceOf[A]
                else
                  io.circe.jawn.decodeByteBuffer[A](deSer.deserialize(topic, data)) match {
                    case Left(value)  => throw value
                    case Right(value) => value
                  }
            }
        }
    }
}
