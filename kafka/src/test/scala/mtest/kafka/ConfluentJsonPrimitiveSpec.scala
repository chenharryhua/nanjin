package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.serdes.ConfluentJsonPrimitive
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serde
import munit.FunSuite

/** Offline round-trip tests for `ConfluentJsonPrimitive`, exercising the Confluent-JSON wire format (magic
  * byte + 4-byte schema id + JSON scalar payload) against a `MockSchemaRegistryClient` so no broker or live
  * registry is required.
  */
class ConfluentJsonPrimitiveSpec extends FunSuite {

  private val TOPIC: String = "confluent.json.primitive.unit.test"

  private val props: Map[String, String] =
    Map(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "mock://confluent-json-primitive")

  /** Build a value-side serde for `A` against a fresh mock registry. */
  private def serde[A](cjp: ConfluentJsonPrimitive[A]): Serde[A] =
    cjp.asValue(new MockSchemaRegistryClient, props).serde

  /** Serialize then deserialize a single value through one serde. */
  private def roundTrip[A](cjp: ConfluentJsonPrimitive[A], value: A): A = {
    val s = serde(cjp)
    s.deserializer.deserialize(TOPIC, s.serializer.serialize(TOPIC, value))
  }

  /** The Confluent wire format: a `0x00` magic byte followed by a 4-byte schema id. */
  private def assertConfluentFramed(bytes: Array[Byte]): Unit = {
    assert(bytes.length >= 5, s"expected at least 5 bytes, got ${bytes.length}")
    assertEquals(bytes(0), 0x00.toByte, "first byte should be the 0x00 magic byte")
  }

  test("1.string round-trips") {
    assertEquals(roundTrip(ConfluentJsonPrimitive[String], "the-key"), "the-key")
  }

  test("2.int round-trips") {
    assertEquals(roundTrip(ConfluentJsonPrimitive[java.lang.Integer], Int.box(42)), Int.box(42))
  }

  test("3.long round-trips") {
    assertEquals(
      roundTrip(ConfluentJsonPrimitive[java.lang.Long], Long.box(9876543210L)),
      Long.box(9876543210L))
  }

  test("4.float round-trips") {
    assertEquals(roundTrip(ConfluentJsonPrimitive[java.lang.Float], Float.box(3.5f)), Float.box(3.5f))
  }

  test("5.double round-trips") {
    assertEquals(
      roundTrip(ConfluentJsonPrimitive[java.lang.Double], Double.box(2.718281828)),
      Double.box(2.718281828))
  }

  test("6.boolean round-trips") {
    assertEquals(roundTrip(ConfluentJsonPrimitive[java.lang.Boolean], Boolean.box(true)), Boolean.box(true))
  }

  test("7.serialized bytes carry the Confluent frame") {
    val s = serde(ConfluentJsonPrimitive[java.lang.Long])
    assertConfluentFramed(s.serializer.serialize(TOPIC, Long.box(1L)))
  }

  test("8.null value serializes to null (tombstone)") {
    val s = serde(ConfluentJsonPrimitive[String])
    assertEquals(s.serializer.serialize(TOPIC, null), null)
  }

  test("9.null bytes deserialize to null (tombstone)") {
    val s = serde(ConfluentJsonPrimitive[java.lang.Integer])
    assertEquals(s.deserializer.deserialize(TOPIC, null), null)
  }

  test("10.key-side serde also round-trips") {
    val client = new MockSchemaRegistryClient
    val s = ConfluentJsonPrimitive[java.lang.Integer].asKey(client, props).serde
    assertEquals(s.deserializer.deserialize(TOPIC, s.serializer.serialize(TOPIC, Int.box(7))), Int.box(7))
  }

  test("11.a whole-number payload still decodes as the requested long") {
    // JSON has one number type; the deserializer target type pins it back to Long even for a small value.
    assertEquals(roundTrip(ConfluentJsonPrimitive[java.lang.Long], Long.box(5L)), Long.box(5L))
  }

  test("12.a wrong-typed payload is rejected with a SerializationException") {
    // Share one registry so the boolean-framed bytes carry a schema id the long serde can resolve.
    // The boolean value is neither null nor a Number, so the long decode's catch-all rejects it.
    val client = new MockSchemaRegistryClient
    val boolSerde = ConfluentJsonPrimitive[java.lang.Boolean].asValue(client, props).serde
    val longSerde = ConfluentJsonPrimitive[java.lang.Long].asValue(client, props).serde

    val boolBytes = boolSerde.serializer.serialize(TOPIC, Boolean.box(true))
    intercept[SerializationException](longSerde.deserializer.deserialize(TOPIC, boolBytes))
  }

  test("13.serializer and deserializer close") {
    val s = serde(ConfluentJsonPrimitive[String])
    s.serializer.close()
    s.deserializer.close()
  }
}
