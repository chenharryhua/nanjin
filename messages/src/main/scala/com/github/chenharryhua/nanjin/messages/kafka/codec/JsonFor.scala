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
  protected def serde: Serde[A]
}

object JsonFor {
  def apply[A](implicit ev: JsonFor[A]): JsonFor[A] = ev

  private val mapper = new ObjectMapper() with ClassTagExtensions
  mapper.registerModule(DefaultScalaModule)

  private def buildSchema(klass: Class[?]): JsonSchema =
    new JsonSchema(new JsonSchemaGenerator(mapper).generateJsonSchema(klass))

  implicit val jsonForString: JsonFor[String] = new JsonFor[String] {
    override def jsonSchema: JsonSchema = buildSchema(classOf[String])
    override protected def serde: Serde[String] = serializable.stringSerde
  }

  implicit val jsonForLong: JsonFor[Long] = new JsonFor[Long] {
    override def jsonSchema: JsonSchema = buildSchema(classOf[Long])
    override protected def serde: Serde[Long] = serializable.longSerde
  }

  implicit val jsonForInt: JsonFor[Int] = new JsonFor[Int] {
    override def jsonSchema: JsonSchema = buildSchema(classOf[Int])
    override protected def serde: Serde[Int] = serializable.intSerde
  }

  implicit val jsonForUUID: JsonFor[UUID] = new JsonFor[UUID] {
    override def jsonSchema: JsonSchema = buildSchema(classOf[UUID])
    override protected def serde: Serde[UUID] = serializable.uuidSerde
  }

  implicit def jsonForClassTag[A: ClassTag]: JsonFor[A] = new JsonFor[A] {
    val jsonSchema: JsonSchema =
      buildSchema(implicitly[ClassTag[A]].runtimeClass)

    override protected val serde: Serde[A] =
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
            @transient private[this] lazy val deSer = new KafkaJsonSchemaDeserializer[A]()

            override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
              deSer.configure(configs, isKey)

            override def close(): Unit = deSer.close()

            override def deserialize(topic: String, data: Array[Byte]): A =
              deSer.deserialize(topic, data)
          }
      }
  }
}
