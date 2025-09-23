package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator
import io.circe.Encoder as JsonEncoder
import io.confluent.kafka.schemaregistry.json.{JsonSchema, JsonSchemaUtils}
import io.confluent.kafka.serializers.json.{KafkaJsonSchemaDeserializer, KafkaJsonSchemaSerializer}
import io.estatico.newtype.macros.newtype
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import java.util
import java.util.UUID
import scala.reflect.ClassTag

sealed trait JsonSchemaFor[A] extends RegisterSerde[A]

object JsonSchemaFor {
  def apply[A](implicit ev: JsonSchemaFor[A]): JsonSchemaFor[A] = macro imp.summon[JsonSchemaFor[A]]

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

  private def buildSchema(klass: Class[?]): JsonSchema =
    new JsonSchema(new JsonSchemaGenerator(mapper).generateJsonSchema(klass))

  /*
   * Specific
   */
  implicit object jsonSchemaForString extends JsonSchemaFor[String] {
    override protected val unregisteredSerde: Serde[String] = serializable.stringSerde
  }

  implicit object jsonSchemaForLong extends JsonSchemaFor[Long] {
    override protected val unregisteredSerde: Serde[Long] = serializable.longSerde
  }

  implicit object jsonSchemaForInt extends JsonSchemaFor[Int] {
    override protected val unregisteredSerde: Serde[Int] = serializable.intSerde
  }

  implicit object jsonSchemaForUUID extends JsonSchemaFor[UUID] {
    override protected val unregisteredSerde: Serde[UUID] = serializable.uuidSerde
  }

  implicit object jsonSchemaForUniversal extends JsonSchemaFor[Universal] {
    override protected val unregisteredSerde: Serde[Universal] =
      new Serde[Universal] with Serializable {
        override val serializer: Serializer[Universal] =
          new Serializer[Universal] with Serializable {
            override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit = ()
            override def close(): Unit = ()
            override def serialize(topic: String, data: Universal): Array[Byte] =
              throw ForbiddenProduceException("JsonSchema")
          }

        override val deserializer: Deserializer[Universal] =
          new Deserializer[Universal] with Serializable {
            @transient private[this] lazy val deSer = new KafkaJsonSchemaDeserializer[JsonNode]()

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
  implicit def jsonSchemaForClassTag[A: ClassTag]: JsonSchemaFor[A] = new JsonSchemaFor[A] {
    override protected val unregisteredSerde: Serde[A] =
      new Serde[A] with Serializable {
        override val serializer: Serializer[A] =
          new Serializer[A] with Serializable {
            @transient private[this] lazy val ser = new KafkaJsonSchemaSerializer[JsonNode]()

            override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
              ser.configure(configs, isKey)

            override def close(): Unit = ser.close()

            override def serialize(topic: String, data: A): Array[Byte] =
              if (data == null) null
              else {
                val payload: JsonNode = mapper.valueToTree[JsonNode](data)
                ser.serialize(
                  topic,
                  JsonSchemaUtils.envelope(buildSchema(implicitly[ClassTag[A]].runtimeClass), payload))
              }
          }

        override val deserializer: Deserializer[A] =
          new Deserializer[A] with Serializable {
            @transient private[this] lazy val deSer = new KafkaJsonSchemaDeserializer[JsonNode]()

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
