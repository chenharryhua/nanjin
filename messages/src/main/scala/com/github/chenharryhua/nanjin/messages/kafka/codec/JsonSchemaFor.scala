package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.github.chenharryhua.nanjin.messages.kafka.globalObjectMapper
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator
import io.circe.DecodingFailure.Reason.CustomReason
import io.circe.{Decoder as JsonDecoder, DecodingFailure, Encoder as JsonEncoder, HCursor}
import io.confluent.kafka.schemaregistry.json.{JsonSchema, JsonSchemaUtils}
import io.confluent.kafka.serializers.json.{
  KafkaJsonSchemaDeserializer,
  KafkaJsonSchemaDeserializerConfig,
  KafkaJsonSchemaSerializer
}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import java.util
import java.util.UUID
import scala.jdk.CollectionConverters.{MapHasAsJava, MapHasAsScala}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

sealed trait JsonSchemaFor[A] extends RegisterSerde[A]

object JsonSchemaFor {
  def apply[A](implicit ev: JsonSchemaFor[A]): JsonSchemaFor[A] = macro imp.summon[JsonSchemaFor[A]]

  final class Universal(val value: JsonNode)
  object Universal {
    implicit val jsonEncoderUniversal: JsonEncoder[Universal] =
      (a: Universal) =>
        io.circe.jawn.parse(globalObjectMapper.writeValueAsString(a.value)) match {
          case Left(value)  => throw value
          case Right(value) => value
        }
    implicit val decoderUniversal: JsonDecoder[Universal] =
      (c: HCursor) =>
        Try(globalObjectMapper.convertValue[JsonNode](c.value.noSpaces)) match {
          case Failure(ex)    => Left(DecodingFailure(CustomReason(ExceptionUtils.getMessage(ex)), c.history))
          case Success(value) => Right(new Universal(value))
        }
  }

  private def buildSchema(klass: Class[?]): JsonSchema =
    new JsonSchema(new JsonSchemaGenerator(globalObjectMapper).generateJsonSchema(klass))

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
            @transient private[this] lazy val ser = new KafkaJsonSchemaSerializer[JsonNode]()

            override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
              ser.configure(configs, isKey)

            override def close(): Unit = ser.close()

            override def serialize(topic: String, data: Universal): Array[Byte] =
              Option(data).flatMap(u => Option(u.value)).map(jn => ser.serialize(topic, jn)).orNull
          }

        override val deserializer: Deserializer[Universal] =
          new Deserializer[Universal] with Serializable {
            @transient private[this] lazy val deSer = new KafkaJsonSchemaDeserializer[JsonNode]()

            override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit = {
              val sm = configs.asScala.toMap
              val newConfig: Map[String, Any] =
                if (isKey)
                  sm.updated(KafkaJsonSchemaDeserializerConfig.JSON_KEY_TYPE, classOf[JsonNode].getName)
                else
                  sm.updated(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, classOf[JsonNode].getName)

              deSer.configure(newConfig.asJava, isKey)
            }

            override def close(): Unit = deSer.close()

            override def deserialize(topic: String, data: Array[Byte]): Universal =
              Option(deSer.deserialize(topic, data)).map(new Universal(_)).orNull
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
              Option(data).map { a =>
                val payload: JsonNode = globalObjectMapper.valueToTree[JsonNode](a)
                val enveloped: ObjectNode =
                  JsonSchemaUtils.envelope(buildSchema(implicitly[ClassTag[A]].runtimeClass), payload)
                ser.serialize(topic, enveloped)
              }.orNull
          }

        override val deserializer: Deserializer[A] =
          new Deserializer[A] with Serializable {
            @transient private[this] lazy val deSer = new KafkaJsonSchemaDeserializer[JsonNode]()

            override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit = {
              val sm = configs.asScala.toMap
              val newConfig: Map[String, Any] =
                if (isKey)
                  sm.updated(KafkaJsonSchemaDeserializerConfig.JSON_KEY_TYPE, classOf[JsonNode].getName)
                else
                  sm.updated(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, classOf[JsonNode].getName)

              deSer.configure(newConfig.asJava, isKey)
            }

            override def close(): Unit = deSer.close()

            @SuppressWarnings(Array("AsInstanceOf"))
            override def deserialize(topic: String, data: Array[Byte]): A = {
              val jn: JsonNode = deSer.deserialize(topic, data)
              if (jn == null) null.asInstanceOf[A] else globalObjectMapper.convertValue[A](jn)
            }
          }
      }
  }
}
