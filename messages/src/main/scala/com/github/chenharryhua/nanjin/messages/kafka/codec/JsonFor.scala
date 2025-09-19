package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator
import io.confluent.kafka.schemaregistry.json.{JsonSchema, JsonSchemaUtils}
import io.confluent.kafka.serializers.json.{KafkaJsonSchemaDeserializer, KafkaJsonSchemaSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

import java.util
import scala.reflect.ClassTag

final class JsonFor[A: ClassTag] private extends RegisterSerde[A] {
  private val mapper = new ObjectMapper() with ClassTagExtensions
  mapper.registerModule(DefaultScalaModule)

  private val schema: JsonSchema =
    new JsonSchema(new JsonSchemaGenerator(mapper).generateJsonSchema(implicitly[ClassTag[A]].runtimeClass))

  override protected val serializer: Serializer[A] =
    new Serializer[A] with Serializable {
      @transient private[this] lazy val ser = new KafkaJsonSchemaSerializer[JsonNode]()

      override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
        ser.configure(configs, isKey)

      override def close(): Unit = ser.close()

      override def serialize(topic: String, data: A): Array[Byte] =
        if (data == null) null
        else {
          val payload: JsonNode = mapper.valueToTree[JsonNode](data)
          ser.serialize(topic, JsonSchemaUtils.envelope(schema, payload))
        }
    }

  override protected val deserializer: Deserializer[A] =
    new Deserializer[A] with Serializable {
      @transient private[this] lazy val deSer = new KafkaJsonSchemaDeserializer[A]()

      override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
        deSer.configure(configs, isKey)

      override def close(): Unit = deSer.close()

      override def deserialize(topic: String, data: Array[Byte]): A =
        deSer.deserialize(topic, data)
    }
}

object JsonFor {
  def apply[A: ClassTag]: JsonFor[A] = new JsonFor[A]
}
