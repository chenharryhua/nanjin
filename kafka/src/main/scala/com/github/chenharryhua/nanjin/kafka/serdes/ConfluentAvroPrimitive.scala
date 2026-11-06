package com.github.chenharryhua.nanjin.kafka.serdes

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

/** An `Unregistered[A]` for a Kafka primitive whose bytes are the Confluent wire format: a 1-byte `0x00`
  * magic byte plus a 4-byte schema id, then the Avro-encoded primitive payload.
  *
  * Unlike `Primitive` (which uses Kafka's built-in `Serdes` and emits a raw payload with no magic byte or
  * schema id, and so needs no schema registry), `ConfluentAvroPrimitive` serializes through Confluent's
  * `KafkaAvroSerializer`/`KafkaAvroDeserializer`. The primitive's Avro schema is registered with (or looked
  * up in) the supplied `SchemaRegistryClient`, so schema compatibility and evolution are governed by the
  * registry. This is the same framing `Structured.avro` produces for `GenericRecord`, applied to scalar
  * values, which makes these primitives interoperable with the Confluent-framed generic connectors.
  *
  * Instances are provided for the scalar types that round-trip through Avro primitive framing; summon one
  * with `ConfluentAvroPrimitive[A]`. `null` values serialize to `null` (a tombstone). Avro has no native
  * `short`, `UUID`, or raw-`bytes`-without-length primitive that round-trips this framing, so no instances
  * are offered for those; use `Primitive` for the built-in-`Serdes` encoding of those types.
  */
sealed trait ConfluentAvroPrimitive[A] extends Unregistered[A]

object ConfluentAvroPrimitive {

  /** Summon the `ConfluentAvroPrimitive` instance for `A`. */
  inline def apply[A](using ev: ConfluentAvroPrimitive[A]): ConfluentAvroPrimitive[A] = ev

  /** Build a Confluent-Avro `Serde[A]` for a primitive. `serialize` passes the boxed value straight to
    * `KafkaAvroSerializer` (which derives the matching Avro primitive schema and emits the Confluent frame).
    * On deserialize, the value is narrowed to `A` by `decode`: a `null` payload (tombstone) passes through as
    * `null`, and an unexpected runtime type is rejected with a `SerializationException`. `null` values
    * serialize to `null`.
    */
  private def serde[A](srClient: SchemaRegistryClient, decode: Any => A): Serde[A] =
    new Serde[A] {
      override lazy val serializer: Serializer[A] =
        new Serializer[A] {
          private val ser: KafkaAvroSerializer = new KafkaAvroSerializer(srClient)

          override def serialize(topic: String, headers: Headers, data: A): Array[Byte] =
            ser.serialize(topic, headers, data)

          override def serialize(topic: String, data: A): Array[Byte] =
            serialize(topic, null, data)

          override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit =
            ser.configure(configs, isKey)

          override def close(): Unit = ser.close()
        }

      override lazy val deserializer: Deserializer[A] =
        new Deserializer[A] {
          private val deSer: KafkaAvroDeserializer = new KafkaAvroDeserializer(srClient)

          override def deserialize(topic: String, headers: Headers, data: Array[Byte]): A =
            decode(deSer.deserialize(topic, headers, data))

          override def deserialize(topic: String, data: Array[Byte]): A =
            deserialize(topic, null, data)

          override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit =
            deSer.configure(configs, isKey)

          override def close(): Unit = deSer.close()
        }
    }

  /** Reject a deserialized value whose runtime type does not match the schema the serde was built for. */
  private def typeError(value: Any, expected: String): Nothing =
    throw new SerializationException( // scalafix:ok
      s"expected a Confluent-Avro $expected but got ${value.getClass.getName}")

  /** Confluent-Avro string primitive. The `KafkaAvroDeserializer` returns Avro's `Utf8` (a `CharSequence`)
    * for a string schema, so a non-null value is rendered to `java.lang.String` with `toString`.
    */
  given ConfluentAvroPrimitive[String] = new ConfluentAvroPrimitive[String] {
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[String] =
      serde(
        srClient,
        {
          case null             => null
          case cs: CharSequence => cs.toString
          case other            => typeError(other, "string")
        })
  }

  given ConfluentAvroPrimitive[java.lang.Integer] = new ConfluentAvroPrimitive[java.lang.Integer] {
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[java.lang.Integer] =
      serde(
        srClient,
        {
          case null                 => null
          case i: java.lang.Integer => i
          case other                => typeError(other, "int")
        })
  }

  given ConfluentAvroPrimitive[java.lang.Long] = new ConfluentAvroPrimitive[java.lang.Long] {
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[java.lang.Long] =
      serde(
        srClient,
        {
          case null              => null
          case l: java.lang.Long => l
          case other             => typeError(other, "long")
        })
  }

  given ConfluentAvroPrimitive[java.lang.Float] = new ConfluentAvroPrimitive[java.lang.Float] {
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[java.lang.Float] =
      serde(
        srClient,
        {
          case null               => null
          case f: java.lang.Float => f
          case other              => typeError(other, "float")
        })
  }

  given ConfluentAvroPrimitive[java.lang.Double] = new ConfluentAvroPrimitive[java.lang.Double] {
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[java.lang.Double] =
      serde(
        srClient,
        {
          case null                => null
          case d: java.lang.Double => d
          case other               => typeError(other, "double")
        })
  }

  given ConfluentAvroPrimitive[java.lang.Boolean] = new ConfluentAvroPrimitive[java.lang.Boolean] {
    override protected def registerWith(srClient: SchemaRegistryClient): Serde[java.lang.Boolean] =
      serde(
        srClient,
        {
          case null                 => null
          case b: java.lang.Boolean => b
          case other                => typeError(other, "boolean")
        })
  }
}
