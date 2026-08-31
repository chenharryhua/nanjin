package com.github.chenharryhua.nanjin.kafka.serdes

import com.fasterxml.jackson.databind.JsonNode
import com.google.protobuf.DynamicMessage
import io.circe.Json
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.json.{
  KafkaJsonSchemaDeserializer,
  KafkaJsonSchemaDeserializerConfig,
  KafkaJsonSchemaSerializer,
  KafkaJsonSchemaSerializerConfig
}
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.given

/** Kafka serdes for structured data formats.
  *
  * Unlike `Primitive` (which handles scalar Kafka types such as String, Integer, or byte arrays),
  * `Structured` covers formats whose payloads carry internal structure: fields, nesting, and (optionally)
  * schema metadata.
  *
  * Available instances:
  *   - `avro` – Avro `GenericRecord`, using Confluent's Schema Registry.
  *   - `jsonSchema` – Jackson `JsonNode`, using Confluent's JSON Schema support.
  *   - `protobuf` – Protobuf `DynamicMessage`, using Confluent's Protobuf support.
  *   - `circe` – Circe `Json`, serialized as compact UTF-8 JSON without schema registry interaction.
  *
  * Each instance can be further transformed via `.become[B]`, `.option`, or `.emap` inherited from
  * `Unregistered`.
  */
sealed trait Structured[A] extends Unregistered[A]

object Structured:
  inline def apply[A](using ev: Structured[A]): Structured[A] = ev

  /** Avro serde backed by Confluent's Schema Registry. Serializes and deserializes `GenericRecord`. */
  given avro: Structured[GenericRecord] = new Structured[GenericRecord]:
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[GenericRecord] =
      new Serde[GenericRecord] {
        override lazy val serializer: Serializer[GenericRecord] =
          new Serializer[GenericRecord]:
            private val ser: KafkaAvroSerializer = new KafkaAvroSerializer(srClient)

            override def serialize(topic: String, headers: Headers, data: GenericRecord): Array[Byte] =
              ser.serialize(topic, headers, data)

            override def serialize(topic: String, data: GenericRecord): Array[Byte] =
              serialize(topic, null, data)

            override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit =
              ser.configure(configs, isKey)

            override def close(): Unit = ser.close()
        end serializer

        override lazy val deserializer: Deserializer[GenericRecord] =
          new Deserializer[GenericRecord]:
            private val deSer: KafkaAvroDeserializer = new KafkaAvroDeserializer(srClient)

            override def deserialize(topic: String, headers: Headers, data: Array[Byte]): GenericRecord =
              deSer.deserialize(topic, headers, data) match
                case null              => null // null first as Kafka semantics (null = tombstone)
                case gr: GenericRecord => gr
                case unknown           =>
                  val str = s"${unknown.getClass.getName} is not a Generic Record"
                  throw new SerializationException(str) // scalafix:ok

            override def deserialize(topic: String, data: Array[Byte]): GenericRecord =
              deserialize(topic, null, data)

            override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit =
              deSer.configure(configs, isKey)

            override def close(): Unit = deSer.close()
        end deserializer
      }
  end avro

  /** JSON Schema serde backed by Confluent's Schema Registry. Serializes and deserializes Jackson `JsonNode`.
    */
  given jsonSchema: Structured[JsonNode] = new Structured[JsonNode]:
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[JsonNode] =
      new Serde[JsonNode]:
        override lazy val serializer: Serializer[JsonNode] =
          new Serializer[JsonNode]:
            private val ser = new KafkaJsonSchemaSerializer[JsonNode](srClient)
            override def serialize(topic: String, data: JsonNode): Array[Byte] =
              ser.serialize(topic, data)

            override def serialize(topic: String, headers: Headers, data: JsonNode): Array[Byte] =
              ser.serialize(topic, headers, data)

            override def close(): Unit = ser.close()

            override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit = {
              val nc =
                configs.asScala.toMap + (KafkaJsonSchemaSerializerConfig.JSON_ENVELOPE_DETECTION -> true)
              ser.configure(nc.asJava, isKey)
            }
        end serializer

        override lazy val deserializer: Deserializer[JsonNode] =
          new Deserializer[JsonNode]:
            private val deSer = new KafkaJsonSchemaDeserializer[JsonNode](srClient)
            override def deserialize(topic: String, data: Array[Byte]): JsonNode =
              deSer.deserialize(topic, data)

            override def deserialize(topic: String, headers: Headers, data: Array[Byte]): JsonNode =
              deSer.deserialize(topic, headers, data)

            override def close(): Unit = deSer.close()

            override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit = {
              val map = configs.asScala.toMap
              val nc =
                if (isKey)
                  map + (KafkaJsonSchemaDeserializerConfig.JSON_KEY_TYPE -> classOf[JsonNode].getName)
                else
                  map + (KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE -> classOf[JsonNode].getName)

              deSer.configure(nc.asJava, isKey)
            }
        end deserializer

  end jsonSchema

  /** Protobuf serde backed by Confluent's Schema Registry. Serializes and deserializes `DynamicMessage`. */
  given protobuf: Structured[DynamicMessage] = new Structured[DynamicMessage]:
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[DynamicMessage] =
      new Serde[DynamicMessage]:
        override lazy val serializer: Serializer[DynamicMessage] =
          new KafkaProtobufSerializer[DynamicMessage](srClient)

        override lazy val deserializer: Deserializer[DynamicMessage] =
          new KafkaProtobufDeserializer[DynamicMessage](srClient)

  end protobuf

  /** Circe JSON serde. Serializes `Json` as compact UTF-8 text without schema registry interaction. */
  given circe: Structured[Json] = new Structured[Json]:
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[Json] =
      new Serde[Json]:
        override lazy val serializer: Serializer[Json] =
          new Serializer[Json]:
            override def serialize(topic: String, headers: Headers, data: Json): Array[Byte] =
              if (data eq null) null
              else data.noSpaces.getBytes(StandardCharsets.UTF_8)

            override def serialize(topic: String, data: Json): Array[Byte] =
              serialize(topic, null, data)

            override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit = ()
            override def close(): Unit = ()

        override lazy val deserializer: Deserializer[Json] =
          new Deserializer[Json]:
            override def deserialize(topic: String, data: Array[Byte]): Json =
              if data eq null then null
              else
                io.circe.jawn.parseByteArray(data) match {
                  case Right(value) => value
                  case Left(ex)     => throw new SerializationException(ex) // scalafix:ok
                }

            override def deserialize(topic: String, headers: Headers, data: Array[Byte]): Json =
              deserialize(topic, data)

            override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit = ()
            override def close(): Unit = ()
  end circe
