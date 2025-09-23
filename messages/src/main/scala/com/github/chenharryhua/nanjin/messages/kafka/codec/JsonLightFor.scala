package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
import io.circe.Encoder as JsonEncoder
import io.confluent.kafka.serializers.{KafkaJsonDeserializer, KafkaJsonSerializer}
import io.estatico.newtype.macros.newtype
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import java.util
import java.util.UUID
import scala.reflect.ClassTag

trait JsonLightFor[A] extends RegisterSerde[A]

object JsonLightFor {
  def apply[A](implicit ev: JsonLightFor[A]): JsonLightFor[A] = macro imp.summon[JsonLightFor[A]]

  private val mapper = new ObjectMapper() with ClassTagExtensions
  mapper.registerModule(DefaultScalaModule)

  @newtype final case class Universal(value: JsonNode)
  object Universal {
    implicit val jsonEncoderUniversal: JsonEncoder[Universal] =
      (a: Universal) =>
        io.circe.jawn.parse(mapper.writeValueAsString(a.value)) match {
          case Left(value)  => throw value
          case Right(value) => value
        }
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
            override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit = ()
            override def close(): Unit = ()
            override def serialize(topic: String, data: Universal): Array[Byte] =
              throw ForbiddenProduceException("JsonLight")
          }

        override val deserializer: Deserializer[Universal] =
          new Deserializer[Universal] with Serializable {
            @transient private[this] lazy val deSer = new KafkaJsonDeserializer[JsonNode]()

            override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
              deSer.configure(configs, isKey)

            override def close(): Unit = deSer.close()

            override def deserialize(topic: String, data: Array[Byte]): Universal =
              if (data == null) null.asInstanceOf[Universal]
              else
                Universal(mapper.convertValue[JsonNode](deSer.deserialize(topic, data)))
          }
      }
  }

  /*
   * General
   */

  implicit def jsonLightForClassTag[A: ClassTag]: JsonLightFor[A] = new JsonLightFor[A] {

    override protected val unregisteredSerde: Serde[A] =
      new Serde[A] with Serializable {
        override val serializer: Serializer[A] =
          new Serializer[A] with Serializable {
            @transient private[this] lazy val ser = new KafkaJsonSerializer[JsonNode]()

            override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
              ser.configure(configs, isKey)

            override def close(): Unit = ser.close()

            override def serialize(topic: String, data: A): Array[Byte] =
              if (data == null) null
              else ser.serialize(topic, mapper.valueToTree[JsonNode](data))
          }

        override val deserializer: Deserializer[A] =
          new Deserializer[A] with Serializable {
            @transient private[this] lazy val deSer = new KafkaJsonDeserializer[JsonNode]()

            override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
              deSer.configure(configs, isKey)

            override def close(): Unit = deSer.close()

            override def deserialize(topic: String, data: Array[Byte]): A =
              if (data == null) null.asInstanceOf[A]
              else
                mapper.convertValue[A](deSer.deserialize(topic, data))
          }
      }
  }
}
