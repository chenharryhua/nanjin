package com.github.chenharryhua.nanjin.kafka.serdes

import com.fasterxml.jackson.databind.JsonNode
import com.google.protobuf.DynamicMessage
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.json.{KafkaJsonSchemaDeserializer, KafkaJsonSchemaSerializer}
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

sealed trait Structured[A] extends Unregistered[A]

object Structured:
  inline def apply[A](using ev: Structured[A]): Structured[A] = ev

  given Structured[GenericRecord] = new Structured[GenericRecord]:
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[GenericRecord] =
      new Serde[GenericRecord]:
        override val serializer: Serializer[GenericRecord] = new Serializer[GenericRecord] {
          private val ser = new KafkaAvroSerializer(srClient)
          override def serialize(topic: String, data: GenericRecord): Array[Byte] =
            ser.serialize(topic, data)

          override def serialize(topic: String, headers: Headers, data: GenericRecord): Array[Byte] =
            ser.serialize(topic, headers, data)

          override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit =
            ser.configure(configs, isKey)

          override def close(): Unit = ser.close()
        }

        override val deserializer: Deserializer[GenericRecord] = new Deserializer[GenericRecord] {
          private val deSer = new KafkaAvroDeserializer(srClient)
          override def deserialize(topic: String, data: Array[Byte]): GenericRecord =
            deSer.deserialize(topic, data) match
              case gr: GenericRecord => gr
              case null              => null
              case unknown           =>
                throw new SerializationException(
                  s"${unknown.toString} is not a Generic Record"
                ) // scalafix:ok

          override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit =
            deSer.configure(configs, isKey)

          override def close(): Unit = deSer.close()
        }
  end given

  given Structured[JsonNode] = new Structured[JsonNode]:
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[JsonNode] =
      new Serde[JsonNode]:
        override val serializer: Serializer[JsonNode] =
          new KafkaJsonSchemaSerializer[JsonNode](srClient)

        override val deserializer: Deserializer[JsonNode] =
          new KafkaJsonSchemaDeserializer[JsonNode](srClient)
  end given

  given Structured[DynamicMessage] = new Structured[DynamicMessage]:
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[DynamicMessage] =
      new Serde[DynamicMessage]:
        override val serializer: Serializer[DynamicMessage] =
          new KafkaProtobufSerializer[DynamicMessage](srClient)

        override val deserializer: Deserializer[DynamicMessage] =
          new KafkaProtobufDeserializer[DynamicMessage](srClient)

  end given
