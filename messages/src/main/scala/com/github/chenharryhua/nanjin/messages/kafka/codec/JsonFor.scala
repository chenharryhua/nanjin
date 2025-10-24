package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.implicits.catsSyntaxOptionId
import com.fasterxml.jackson.databind.JsonNode
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
import io.estatico.newtype.macros.newtype
import io.estatico.newtype.ops.toCoercibleIdOps
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.util
import java.util.UUID
import scala.jdk.CollectionConverters.{MapHasAsJava, MapHasAsScala}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

sealed trait JsonFor[A] extends UnregisteredSerde[A] {
  def jsonSchema: Option[JsonSchema]
}

object JsonFor {
  def apply[A](implicit ev: JsonFor[A]): JsonFor[A] = ev

  @newtype final class FromBroker private (val value: JsonNode)
  object FromBroker {
    def apply(jn: JsonNode): FromBroker = jn.coerce
    implicit val jsonEncoderUniversal: JsonEncoder[FromBroker] =
      (a: FromBroker) =>
        io.circe.jawn.parse(globalObjectMapper.writeValueAsString(a.value)) match {
          case Left(value)  => throw value
          case Right(value) => value
        }
    implicit val jsonDecoderUniversal: JsonDecoder[FromBroker] =
      (c: HCursor) =>
        Try(globalObjectMapper.convertValue[JsonNode](c.value.noSpaces)) match {
          case Failure(ex)    => Left(DecodingFailure(CustomReason(ExceptionUtils.getMessage(ex)), c.history))
          case Success(value) => Right(value.coerce)
        }
  }

  private def buildSchema(klass: Class[?]): JsonSchema =
    new JsonSchema(new JsonSchemaGenerator(globalObjectMapper).generateJsonSchema(klass))

  /*
   * Specific
   */
  implicit object jsonForString extends JsonFor[String] {
    override val isPrimitive: Boolean = true

    override protected val unregisteredSerde: Serde[String] = Serdes.stringSerde
    override val jsonSchema: Option[JsonSchema] = buildSchema(classOf[String]).some
  }

  implicit object jsonForLong extends JsonFor[Long] {
    override val isPrimitive: Boolean = true

    override protected val unregisteredSerde: Serde[Long] = Serdes.longSerde
    override val jsonSchema: Option[JsonSchema] = buildSchema(classOf[Long]).some
  }

  implicit object jsonForInt extends JsonFor[Int] {
    override val isPrimitive: Boolean = true

    override protected val unregisteredSerde: Serde[Int] = Serdes.intSerde
    override val jsonSchema: Option[JsonSchema] = buildSchema(classOf[Int]).some
  }

  implicit object jsonForUUID extends JsonFor[UUID] {
    override val isPrimitive: Boolean = true

    override protected val unregisteredSerde: Serde[UUID] = Serdes.uuidSerde
    override val jsonSchema: Option[JsonSchema] = buildSchema(classOf[UUID]).some
  }

  implicit object jsonForFromBroker extends JsonFor[FromBroker] {
    override val isPrimitive: Boolean = false
    override val jsonSchema: Option[JsonSchema] = None

    override protected val unregisteredSerde: Serde[FromBroker] =
      new Serde[FromBroker] {
        override val serializer: Serializer[FromBroker] = new Serializer[FromBroker] {
          private[this] val ser = new KafkaJsonSchemaSerializer[JsonNode]()

          override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
            ser.configure(configs, isKey)

          override def close(): Unit = ser.close()

          override def serialize(topic: String, data: FromBroker): Array[Byte] =
            Option(data).flatMap(u => Option(u.value)).map(jn => ser.serialize(topic, jn)).orNull
        }

        override val deserializer: Deserializer[FromBroker] = new Deserializer[FromBroker] {
          private[this] val deSer = new KafkaJsonSchemaDeserializer[JsonNode]()

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

          override def deserialize(topic: String, data: Array[Byte]): FromBroker =
            Option(deSer.deserialize(topic, data))
              .map(_.coerce[FromBroker])
              .getOrElse(null.asInstanceOf[FromBroker])
        }
      }
  }

  /*
   * General
   */
  implicit def jsonForClassTag[A: ClassTag]: JsonFor[A] = new JsonFor[A] {
    override val isPrimitive: Boolean = false

    private val schema: JsonSchema = buildSchema(implicitly[ClassTag[A]].runtimeClass)
    override val jsonSchema: Option[JsonSchema] = schema.some

    override protected val unregisteredSerde: Serde[A] =
      new Serde[A] {
        override val serializer: Serializer[A] = new Serializer[A] {
          private[this] val ser = new KafkaJsonSchemaSerializer[JsonNode]()

          override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
            ser.configure(configs, isKey)

          override def close(): Unit = ser.close()

          override def serialize(topic: String, data: A): Array[Byte] =
            Option(data).map { a =>
              val payload: JsonNode = globalObjectMapper.valueToTree[JsonNode](a)
              ser.serialize(topic, JsonSchemaUtils.envelope(schema, payload))
            }.orNull
        }

        override val deserializer: Deserializer[A] = new Deserializer[A] {
          private[this] val deSer = new KafkaJsonSchemaDeserializer[JsonNode]()

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

          override def deserialize(topic: String, data: Array[Byte]): A =
            Option(deSer.deserialize(topic, data))
              .map(jn => globalObjectMapper.convertValue[A](jn))
              .getOrElse(null.asInstanceOf[A])
        }
      }
  }
}
