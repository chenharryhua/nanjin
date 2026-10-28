package com.github.chenharryhua.nanjin.kafka.streaming

import com.github.chenharryhua.nanjin.kafka.config.SerdeSettings
import com.github.chenharryhua.nanjin.kafka.serdes.Primitive
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.kafka.common.serialization.Serde
import munit.FunSuite

/** Offline coverage for `StreamsSerde`.
  *
  * `StreamsSerde` wires nanjin `Unregistered` serdes into the Kafka Streams operator configs (`Consumed`,
  * `Produced`, `Joined`, etc.). The `Primitive` serdes used here carry built-in Kafka `Serdes` and ignore the
  * schema registry, so the whole thing runs offline against an in-memory `MockSchemaRegistryClient`.
  *
  * The Streams configs do not expose their serdes publicly, so each test reads the relevant `protected` serde
  * field by reflection and round-trips a value through it. Using a distinct payload type per slot (String
  * key, Long / Integer values) proves each factory places the right serde in the right slot without swapping
  * key and value.
  */
class StreamsSerdeTest extends FunSuite {

  private val serde: StreamsSerde =
    new StreamsSerde(new MockSchemaRegistryClient(), SerdeSettings(Map.empty))

  private val keyP: Primitive[String] = Primitive[String]
  private val longP: Primitive[java.lang.Long] = Primitive[java.lang.Long]
  private val intP: Primitive[java.lang.Integer] = Primitive[java.lang.Integer]

  private val topic = "streams.serde.test"

  /** Read a `protected` `Serde` field (declared on the object's own class) by reflection. */
  private def serdeField(config: Any, name: String): Serde[Any] = {
    val field = config.getClass.getDeclaredField(name)
    field.setAccessible(true)
    field.get(config).asInstanceOf[Serde[Any]]
  }

  /** Assert the serde serializes then deserializes `value` back to an equal value. */
  private def assertRoundTrip[A](s: Serde[Any], value: A): Unit = {
    val bytes = s.serializer().serialize(topic, value)
    val back = s.deserializer().deserialize(topic, bytes)
    assertEquals(back, value.asInstanceOf[Any])
  }

  test("1.consumed places the key and value serdes in their slots") {
    val c = serde.consumed(keyP, longP)
    assertRoundTrip(serdeField(c, "keySerde"), "hello")
    assertRoundTrip(serdeField(c, "valueSerde"), java.lang.Long.valueOf(42L))
  }

  test("2.produced places the key and value serdes in their slots") {
    val p = serde.produced(keyP, longP)
    assertRoundTrip(serdeField(p, "keySerde"), "hello")
    assertRoundTrip(serdeField(p, "valueSerde"), java.lang.Long.valueOf(42L))
  }

  test("3.grouped places the key and value serdes in their slots") {
    val g = serde.grouped(keyP, longP)
    assertRoundTrip(serdeField(g, "keySerde"), "hello")
    assertRoundTrip(serdeField(g, "valueSerde"), java.lang.Long.valueOf(42L))
  }

  test("4.repartitioned places the key and value serdes in their slots") {
    val r = serde.repartitioned(keyP, longP)
    assertRoundTrip(serdeField(r, "keySerde"), "hello")
    assertRoundTrip(serdeField(r, "valueSerde"), java.lang.Long.valueOf(42L))
  }

  test("5.joined places key, left-value and right-value serdes in distinct slots") {
    val j = serde.joined(keyP, longP, intP)
    assertRoundTrip(serdeField(j, "keySerde"), "hello")
    assertRoundTrip(serdeField(j, "leftValueSerde"), java.lang.Long.valueOf(42L))
    assertRoundTrip(serdeField(j, "rightValueSerde"), java.lang.Integer.valueOf(7))
  }

  test("6.streamJoined places key, value and other-value serdes in distinct slots") {
    val sj = serde.streamJoined(keyP, longP, intP)
    assertRoundTrip(serdeField(sj, "keySerde"), "hello")
    assertRoundTrip(serdeField(sj, "valueSerde"), java.lang.Long.valueOf(42L))
    assertRoundTrip(serdeField(sj, "otherValueSerde"), java.lang.Integer.valueOf(7))
  }
}
