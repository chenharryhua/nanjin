package com.github.chenharryhua.nanjin.kafka.serdes

import com.fasterxml.jackson.databind.JsonNode
import com.google.protobuf.DynamicMessage
import io.confluent.kafka.serializers.json.{KafkaJsonSchemaDeserializer, KafkaJsonSchemaSerializer}
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

sealed trait SchemaBased[A] extends Unregistered[A]

object SchemaBased:
  inline def apply[A](using ev: SchemaBased[A]): SchemaBased[A] = ev

  given SchemaBased[GenericRecord] = new SchemaBased[GenericRecord]:
    override protected val unregistered: Serde[GenericRecord] =
      new Serde[GenericRecord]:
        override val serializer: Serializer[GenericRecord] =
          new GenericAvroSerializer
        override val deserializer: Deserializer[GenericRecord] =
          new GenericAvroDeserializer
  end given

  given SchemaBased[JsonNode] = new SchemaBased[JsonNode]:
    override protected val unregistered: Serde[JsonNode] =
      new Serde[JsonNode]:
        override val serializer: Serializer[JsonNode] =
          new KafkaJsonSchemaSerializer[JsonNode]
        override val deserializer: Deserializer[JsonNode] =
          new KafkaJsonSchemaDeserializer[JsonNode]
  end given

  given SchemaBased[DynamicMessage] = new SchemaBased[DynamicMessage]:
    override protected val unregistered: Serde[DynamicMessage] =
      new Serde[DynamicMessage]:
        override val serializer: Serializer[DynamicMessage] =
          new KafkaProtobufSerializer[DynamicMessage]

        override val deserializer: Deserializer[DynamicMessage] =
          new KafkaProtobufDeserializer[DynamicMessage]
  end given
