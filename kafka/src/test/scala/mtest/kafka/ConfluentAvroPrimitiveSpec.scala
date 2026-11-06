package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.serdes.ConfluentAvroPrimitive
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.kafka.common.serialization.Serde
import munit.FunSuite

/** Offline round-trip tests for `ConfluentAvroPrimitive`, exercising the Confluent-Avro wire format (magic
  * byte + 4-byte schema id + Avro primitive payload) against a `MockSchemaRegistryClient` so no broker or
  * live registry is required.
  */
class ConfluentAvroPrimitiveSpec extends FunSuite {

  private val TOPIC: String = "confluent.avro.primitive.unit.test"

  private val props: Map[String, String] =
    Map(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "mock://confluent-avro-primitive")

  /** Build a value-side serde for `A` against a fresh mock registry. */
  private def serde[A](cap: ConfluentAvroPrimitive[A]): Serde[A] =
    cap.asValue(new MockSchemaRegistryClient, props).serde

  /** Serialize then deserialize a single value through one serde. */
  private def roundTrip[A](cap: ConfluentAvroPrimitive[A], value: A): A = {
    val s = serde(cap)
    s.deserializer.deserialize(TOPIC, s.serializer.serialize(TOPIC, value))
  }

  /** The Confluent wire format: a `0x00` magic byte followed by a 4-byte schema id. */
  private def assertConfluentFramed(bytes: Array[Byte]): Unit = {
    assert(bytes.length >= 5, s"expected at least 5 bytes, got ${bytes.length}")
    assertEquals(bytes(0), 0x00.toByte, "first byte should be the 0x00 magic byte")
  }

  test("1.string round-trips") {
    assertEquals(roundTrip(ConfluentAvroPrimitive[String], "the-key"), "the-key")
  }

  test("2.int round-trips") {
    assertEquals(roundTrip(ConfluentAvroPrimitive[java.lang.Integer], Int.box(42)), Int.box(42))
  }

  test("3.long round-trips") {
    assertEquals(
      roundTrip(ConfluentAvroPrimitive[java.lang.Long], Long.box(9876543210L)),
      Long.box(9876543210L))
  }

  test("4.float round-trips") {
    assertEquals(roundTrip(ConfluentAvroPrimitive[java.lang.Float], Float.box(3.5f)), Float.box(3.5f))
  }

  test("5.double round-trips") {
    assertEquals(
      roundTrip(ConfluentAvroPrimitive[java.lang.Double], Double.box(2.718281828)),
      Double.box(2.718281828))
  }

  test("6.boolean round-trips") {
    assertEquals(roundTrip(ConfluentAvroPrimitive[java.lang.Boolean], Boolean.box(true)), Boolean.box(true))
  }

  test("7.serialized bytes carry the Confluent frame") {
    val s = serde(ConfluentAvroPrimitive[java.lang.Long])
    assertConfluentFramed(s.serializer.serialize(TOPIC, Long.box(1L)))
  }

  test("8.null value serializes to null (tombstone)") {
    val s = serde(ConfluentAvroPrimitive[String])
    assertEquals(s.serializer.serialize(TOPIC, null), null)
  }

  test("9.null bytes deserialize to null (tombstone)") {
    val s = serde(ConfluentAvroPrimitive[java.lang.Integer])
    assertEquals(s.deserializer.deserialize(TOPIC, null), null)
  }

  test("10.key-side serde also round-trips") {
    val client = new MockSchemaRegistryClient
    val s = ConfluentAvroPrimitive[java.lang.Integer].asKey(client, props).serde
    assertEquals(s.deserializer.deserialize(TOPIC, s.serializer.serialize(TOPIC, Int.box(7))), Int.box(7))
  }
}
