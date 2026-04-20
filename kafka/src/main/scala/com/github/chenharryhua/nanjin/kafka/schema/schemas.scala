package com.github.chenharryhua.nanjin.kafka.schema

import com.github.chenharryhua.nanjin.kafka.serdes.globalObjectMapper
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator
import com.sksamuel.avro4s.SchemaFor
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.reflect.ClassTag

sealed trait KafkaJsonSchema[A]:
  def schema: JsonSchema

object KafkaJsonSchema:
  def apply[A](using ev: KafkaJsonSchema[A]): KafkaJsonSchema[A] = ev

  given [A: ClassTag]: KafkaJsonSchema[A] = new KafkaJsonSchema {
    override def schema: JsonSchema = new JsonSchema(
      new JsonSchemaGenerator(globalObjectMapper).generateJsonSchema(summon[ClassTag[A]].runtimeClass)
    )
  }
end KafkaJsonSchema

sealed trait KafkaAvroSchema[A]:
  def schema: AvroSchema
object KafkaAvroSchema:
  def apply[A](using ev: KafkaAvroSchema[A]): KafkaAvroSchema[A] = ev

  given [A: SchemaFor]: KafkaAvroSchema[A] = new KafkaAvroSchema[A] {
    override def schema: AvroSchema = AvroSchema(summon[SchemaFor[A]].schema)
  }
end KafkaAvroSchema

sealed trait KafkaProtobufSchema[A]:
  def schema: ProtobufSchema
object KafkaProtobufSchema:
  def apply[A](using ev: KafkaProtobufSchema[A]): KafkaProtobufSchema[A] = ev

  given [A <: GeneratedMessage: GeneratedMessageCompanion]: KafkaProtobufSchema[A] =
    new KafkaProtobufSchema[A] {
      override def schema: ProtobufSchema = ProtobufSchema(
        summon[GeneratedMessageCompanion[A]].javaDescriptor)
    }
end KafkaProtobufSchema
