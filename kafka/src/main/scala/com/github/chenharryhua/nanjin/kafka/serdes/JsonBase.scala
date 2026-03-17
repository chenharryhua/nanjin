package com.github.chenharryhua.nanjin.kafka.serdes

import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.kafka.serdes.Unregistered
import io.confluent.kafka.serializers.json.{
  KafkaJsonSchemaDeserializer,
  KafkaJsonSchemaDeserializerConfig,
  KafkaJsonSchemaSerializer
}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.jdk.CollectionConverters.given

trait JsonBase[A] extends Unregistered[A]

object JsonBase {

  given JsonBase[JsonNode] = new JsonBase[JsonNode] {
    override protected val unregistered: Serde[JsonNode] =
      new Serde[JsonNode] {
        /*
         * Serializer
         */
        override def serializer: Serializer[JsonNode] =
          new Serializer[JsonNode] {
            private val ser = new KafkaJsonSchemaSerializer[JsonNode]

            override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit =
              ser.configure(configs, isKey)

            override def close(): Unit = ser.close()

            export ser.serialize
          }

        /*
         * Deserializer
         */
        override def deserializer: Deserializer[JsonNode] =
          new Deserializer[JsonNode] {
            private val deSer = new KafkaJsonSchemaDeserializer[JsonNode]

            override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit = {
              val sm = configs.asScala.toMap
              val newConfig: Map[String, Any] =
                if (isKey)
                  sm.updated(KafkaJsonSchemaDeserializerConfig.JSON_KEY_TYPE, classOf[JsonNode].getName)
                else
                  sm.updated(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, classOf[JsonNode].getName)

              deSer.configure(newConfig.asJava, isKey)
            }

            override def close(): Unit = deSer.close()

            export deSer.deserialize
          }
      }
  }
}
