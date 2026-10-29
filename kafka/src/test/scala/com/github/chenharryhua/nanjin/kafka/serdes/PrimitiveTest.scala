package com.github.chenharryhua.nanjin.kafka.serdes

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.kafka.common.serialization.Serde
import munit.FunSuite

import java.nio.ByteBuffer
import java.util.UUID

class PrimitiveTest extends FunSuite {

  private val TOPIC = "primitive.test"
  private val CLIENT = new MockSchemaRegistryClient

  private def serde[A: Primitive]: Serde[A] =
    Primitive[A].asValue(CLIENT, Map.empty).serde

  private def round_trip[A: Primitive](value: A): A = {
    val primitive_serde = serde[A]
    primitive_serde.deserializer().deserialize(TOPIC, primitive_serde.serializer().serialize(TOPIC, value))
  }

  test("1.String and numeric Primitive instances round-trip") {
    assertEquals(round_trip("text"), "text")
    assertEquals(round_trip(java.lang.Long.valueOf(42L)), java.lang.Long.valueOf(42L))
    assertEquals(round_trip(java.lang.Integer.valueOf(7)), java.lang.Integer.valueOf(7))
    assertEquals(round_trip(java.lang.Float.valueOf(1.5f)), java.lang.Float.valueOf(1.5f))
    assertEquals(round_trip(java.lang.Double.valueOf(2.5d)), java.lang.Double.valueOf(2.5d))
    assertEquals(round_trip(java.lang.Short.valueOf(3.toShort)), java.lang.Short.valueOf(3.toShort))
  }

  test("2.Byte array, Boolean, and ByteBuffer Primitive instances round-trip") {
    val bytes = Array[Byte](1, 2, 3)
    assert(round_trip(bytes).sameElements(bytes))
    assertEquals(round_trip(java.lang.Boolean.TRUE), java.lang.Boolean.TRUE)

    val byte_buffer = ByteBuffer.wrap(Array[Byte](4, 5, 6))
    assertEquals(round_trip(byte_buffer), byte_buffer)
  }

  test("3.UUID Primitive instance round-trips") {
    val uuid = UUID.fromString("12345678-90ab-cdef-1234-567890abcdef")
    assertEquals(round_trip(uuid), uuid)
  }

  test("4.key and value registration modes both preserve the underlying primitive serde") {
    val key_serde = Primitive[String].asKey(CLIENT, Map("unused" -> "value")).serde
    val value_serde = Primitive[String].asValue(CLIENT, Map("unused" -> "value")).serde

    val key_bytes = key_serde.serializer().serialize(TOPIC, "key")
    val value_bytes = value_serde.serializer().serialize(TOPIC, "value")
    assertEquals(key_serde.deserializer().deserialize(TOPIC, key_bytes), "key")
    assertEquals(value_serde.deserializer().deserialize(TOPIC, value_bytes), "value")
  }
}
