package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator
import io.confluent.kafka.schemaregistry.json.{JsonSchema, JsonSchemaUtils}
import io.confluent.kafka.serializers.json.{KafkaJsonSchemaDeserializer, KafkaJsonSchemaSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import java.util
import java.util.UUID
import scala.reflect.ClassTag

sealed trait JsonFor[A] extends RegisterSerde[A] {
  def jsonSchema: JsonSchema
  protected def unregisteredSerde: Serde[A]
}

object JsonFor {
  def apply[A](implicit ev: JsonFor[A]): JsonFor[A] = ev

  private val mapper = new ObjectMapper() with ClassTagExtensions
  mapper.registerModule(DefaultScalaModule)

  private def buildSchema(klass: Class[?]): JsonSchema =
    new JsonSchema(new JsonSchemaGenerator(mapper).generateJsonSchema(klass))

  implicit object jsonForString extends JsonFor[String] {
    override def jsonSchema: JsonSchema = buildSchema(classOf[String])
    override protected val unregisteredSerde: Serde[String] = serializable.stringSerde
  }

  implicit object jsonForLong extends JsonFor[Long] {
    override def jsonSchema: JsonSchema = buildSchema(classOf[Long])
    override protected val unregisteredSerde: Serde[Long] = serializable.longSerde
  }

  implicit object jsonForInt extends JsonFor[Int] {
    override def jsonSchema: JsonSchema = buildSchema(classOf[Int])
    override protected val unregisteredSerde: Serde[Int] = serializable.intSerde
  }

  implicit object jsonForUUID extends JsonFor[UUID] {
    override def jsonSchema: JsonSchema = buildSchema(classOf[UUID])
    override protected val unregisteredSerde: Serde[UUID] = serializable.uuidSerde
  }

  implicit def jsonForClassTag[A: ClassTag]: JsonFor[A] = new JsonFor[A] {
    val jsonSchema: JsonSchema = buildSchema(implicitly[ClassTag[A]].runtimeClass)

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
                ser.serialize(topic, JsonSchemaUtils.envelope(jsonSchema, payload))
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
