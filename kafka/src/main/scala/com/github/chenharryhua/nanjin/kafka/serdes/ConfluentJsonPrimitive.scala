package com.github.chenharryhua.nanjin.kafka.serdes

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.json.{
  KafkaJsonSchemaDeserializer,
  KafkaJsonSchemaDeserializerConfig,
  KafkaJsonSchemaSerializer,
  KafkaJsonSchemaSerializerConfig
}
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.jdk.CollectionConverters.given

/** An `Unregistered[A]` for a Kafka primitive whose bytes are the Confluent wire format backed by '''JSON
  * Schema''': a 1-byte `0x00` magic byte plus a 4-byte schema id, then the JSON-encoded scalar payload (e.g.
  * `42`, `"foo"`, `true`).
  *
  * This is the JSON-Schema counterpart of `ConfluentAvroPrimitive`. Both emit the Confluent frame and are
  * governed by the registry, but the payload after the 5-byte prefix is JSON text here rather than Avro
  * binary. It serializes through Confluent's `KafkaJsonSchemaSerializer`/`KafkaJsonSchemaDeserializer`; the
  * scalar's JSON Schema (`{"type":"integer"}`, `{"type":"string"}`, and so on) is registered with (or looked
  * up in) the supplied `SchemaRegistryClient`. Unlike `Primitive`, which uses Kafka's built-in `Serdes` and
  * needs no registry, this requires a reachable registry.
  *
  * Because JSON has a single `number` type, the deserializer cannot tell an `integer` from a `long` or a
  * `float` from a `double` from the payload alone. Each instance therefore pins its own target class in
  * `configure` (`JSON_KEY_TYPE`/`JSON_VALUE_TYPE`) so a bare scalar binds back to the right Java type, and
  * then decodes numerics leniently through `java.lang.Number` (so a value Jackson widened or narrowed to a
  * neighbouring numeric type still yields the requested one).
  *
  * Instances are provided for the scalar JSON types; summon one with `ConfluentJsonPrimitive[A]`. `null`
  * values serialize to `null` (a tombstone).
  */
sealed trait ConfluentJsonPrimitive[A] extends Unregistered[A]

object ConfluentJsonPrimitive {

  /** Summon the `ConfluentJsonPrimitive` instance for `A`. */
  inline def apply[A](using ev: ConfluentJsonPrimitive[A]): ConfluentJsonPrimitive[A] = ev

  /** Build a Confluent-JSON `Serde[A]` for a primitive. `serialize` passes the boxed value to
    * `KafkaJsonSchemaSerializer` (which derives the matching JSON Schema and emits the Confluent frame), with
    * `JSON_ENVELOPE_DETECTION` forced on. On deserialize, the target class `targetType` is pinned via
    * `JSON_KEY_TYPE`/`JSON_VALUE_TYPE` so the scalar binds to the right Java type, then `decode` narrows the
    * result to `A`: a `null` payload (tombstone) passes through as `null`, and an unexpected runtime type is
    * rejected with a `SerializationException`. `null` values serialize to `null`.
    */
  private def serde[A](srClient: SchemaRegistryClient, targetType: Class[?], decode: Any => A): Serde[A] =
    new Serde[A] {
      override lazy val serializer: Serializer[A] =
        new Serializer[A] {
          private val ser: KafkaJsonSchemaSerializer[A] = new KafkaJsonSchemaSerializer[A](srClient)

          override def serialize(topic: String, headers: Headers, data: A): Array[Byte] =
            ser.serialize(topic, headers, data)

          override def serialize(topic: String, data: A): Array[Byte] =
            serialize(topic, null, data)

          override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit = {
            val nc =
              configs.asScala.toMap + (KafkaJsonSchemaSerializerConfig.JSON_ENVELOPE_DETECTION -> true)
            ser.configure(nc.asJava, isKey)
          }

          override def close(): Unit = ser.close()
        }

      override lazy val deserializer: Deserializer[A] =
        new Deserializer[A] {
          private val deSer: KafkaJsonSchemaDeserializer[A] = new KafkaJsonSchemaDeserializer[A](srClient)

          override def deserialize(topic: String, headers: Headers, data: Array[Byte]): A =
            decode(deSer.deserialize(topic, headers, data))

          override def deserialize(topic: String, data: Array[Byte]): A =
            deserialize(topic, null, data)

          override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit = {
            val map = configs.asScala.toMap
            val nc =
              if (isKey) map + (KafkaJsonSchemaDeserializerConfig.JSON_KEY_TYPE -> targetType.getName)
              else map + (KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE -> targetType.getName)
            deSer.configure(nc.asJava, isKey)
          }

          override def close(): Unit = deSer.close()
        }
    }

  /** Reject a deserialized value whose runtime type does not match the schema the serde was built for. */
  private def typeError(value: Any, expected: String): Nothing =
    throw new SerializationException( // scalafix:ok
      s"expected a Confluent-JSON $expected but got ${value.getClass.getName}")

  given ConfluentJsonPrimitive[String] = new ConfluentJsonPrimitive[String] {
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[String] =
      serde(
        srClient,
        classOf[String],
        {
          case null             => null
          case cs: CharSequence => cs.toString
          case other            => typeError(other, "string")
        }
      )
  }

  given ConfluentJsonPrimitive[java.lang.Integer] = new ConfluentJsonPrimitive[java.lang.Integer] {
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[java.lang.Integer] =
      serde(
        srClient,
        classOf[java.lang.Integer],
        {
          case null                => null
          case n: java.lang.Number => Int.box(n.intValue)
          case other               => typeError(other, "int")
        }
      )
  }

  given ConfluentJsonPrimitive[java.lang.Long] = new ConfluentJsonPrimitive[java.lang.Long] {
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[java.lang.Long] =
      serde(
        srClient,
        classOf[java.lang.Long],
        {
          case null                => null
          case n: java.lang.Number => Long.box(n.longValue)
          case other               => typeError(other, "long")
        }
      )
  }

  given ConfluentJsonPrimitive[java.lang.Float] = new ConfluentJsonPrimitive[java.lang.Float] {
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[java.lang.Float] =
      serde(
        srClient,
        classOf[java.lang.Float],
        {
          case null                => null
          case n: java.lang.Number => Float.box(n.floatValue)
          case other               => typeError(other, "float")
        }
      )
  }

  given ConfluentJsonPrimitive[java.lang.Double] = new ConfluentJsonPrimitive[java.lang.Double] {
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[java.lang.Double] =
      serde(
        srClient,
        classOf[java.lang.Double],
        {
          case null                => null
          case n: java.lang.Number => Double.box(n.doubleValue)
          case other               => typeError(other, "double")
        }
      )
  }

  given ConfluentJsonPrimitive[java.lang.Boolean] = new ConfluentJsonPrimitive[java.lang.Boolean] {
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[java.lang.Boolean] =
      serde(
        srClient,
        classOf[java.lang.Boolean],
        {
          case null                 => null
          case b: java.lang.Boolean => b
          case other                => typeError(other, "boolean")
        }
      )
  }
}
