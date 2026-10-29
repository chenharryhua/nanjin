package com.github.chenharryhua.nanjin.kafka.streaming

import com.github.chenharryhua.nanjin.kafka.{TopicName, TopicSerde}
import com.github.chenharryhua.nanjin.kafka.serdes.KafkaSerde
import org.apache.kafka.common.serialization.Serdes
import munit.FunSuite

import java.time.Duration
import scala.concurrent.duration.DurationInt

class StateStoresTest extends FunSuite {

  private val topicName: TopicName = TopicName("state.store.test")

  private val topic: TopicSerde[String, String] =
    TopicSerde(
      topicName,
      new KafkaSerde[String](Serdes.String(), topicName),
      new KafkaSerde[String](Serdes.String(), topicName))

  private val stores: StateStores[String, String] = StateStores(topic)

  test("1.store name is the topic name") {
    assert(stores.name == topicName.value)
  }

  test("2.persistent key-value store supplier carries the store name and builders are available") {
    val helper = stores.persistentKeyValueStore
    assert(helper.supplier.name() == topicName.value)
    assert(helper.keyValueStoreBuilder.name() == topicName.value)
    assert(helper.timestampedKeyValueStoreBuilder.name() == topicName.value)
    assert(helper.materialized != null)
  }

  test("3.in-memory and lru key-value stores are available") {
    assert(stores.inMemoryKeyValueStore.supplier.name() == topicName.value)
    assert(stores.persistentTimestampedKeyValueStore.supplier.name() == topicName.value)
    assert(stores.lruMap(100).supplier.name() == topicName.value)
  }

  test("4.window stores build with the given retention and window size") {
    val helper =
      stores.persistentWindowStore(Duration.ofHours(1), Duration.ofMinutes(5), retainDuplicates = false)
    assert(helper.supplier.name() == topicName.value)
    assert(helper.windowStoreBuilder.name() == topicName.value)
    assert(helper.timestampedWindowStoreBuilder.name() == topicName.value)
    // in-memory and timestamped variants are also available
    assert(
      stores.inMemoryWindowStore(
        Duration.ofHours(1),
        Duration.ofMinutes(5),
        false).supplier.name() == topicName.value)
    assert(
      stores.persistentTimestampedWindowStore(Duration.ofHours(1), Duration.ofMinutes(5), false)
        .supplier.name() == topicName.value)
  }

  test("5.session stores build with the given retention") {
    assert(stores.persistentSessionStore(Duration.ofHours(1)).sessionStoreBuilder.name() == topicName.value)
    assert(stores.inMemorySessionStore(Duration.ofHours(1)).supplier.name() == topicName.value)
  }

  test("6.query parameters carry the store name") {
    assert(stores.queries.keyValueStore.storeName() == topicName.value)
    assert(stores.queries.timestampedKeyValueStore.storeName() == topicName.value)
    assert(stores.queries.windowStore.storeName() == topicName.value)
    assert(stores.queries.timestampedWindowStore.storeName() == topicName.value)
    assert(stores.queries.sessionStore.storeName() == topicName.value)
  }

  test("7.timestamped key-value stores expose builders and Materialized") {
    val helper = stores.persistentTimestampedKeyValueStore
    assertEquals(helper.supplier.name(), topicName.value)
    assertEquals(helper.keyValueStoreBuilder.name(), topicName.value)
    assertEquals(helper.timestampedKeyValueStoreBuilder.name(), topicName.value)
    assert(helper.materialized != null)
  }

  test("8.window and session helpers expose Materialized views") {
    val window = stores.persistentWindowStore(Duration.ofHours(1), Duration.ofMinutes(5), false)
    val session = stores.persistentSessionStore(Duration.ofHours(1))

    assert(window.materialized != null)
    assert(session.materialized != null)
  }

  test("9.FiniteDuration overloads build every supported window and session store") {
    val persistent = stores.persistentWindowStore(1.hour, 5.minutes, false)
    val timestamped = stores.persistentTimestampedWindowStore(1.hour, 5.minutes, false)
    val in_memory = stores.inMemoryWindowStore(1.hour, 5.minutes, false)
    val in_memory_session = stores.inMemorySessionStore(1.hour)

    assertEquals(persistent.supplier.name(), topicName.value)
    assertEquals(timestamped.supplier.name(), topicName.value)
    assertEquals(in_memory.supplier.name(), topicName.value)
    assertEquals(in_memory_session.supplier.name(), topicName.value)
  }

  test("10.persistent timestamped window store supports the Java Duration overload") {
    val helper =
      stores.persistentTimestampedWindowStore(Duration.ofHours(1), Duration.ofMinutes(5), false)

    assertEquals(helper.supplier.name(), topicName.value)
    assertEquals(helper.timestampedWindowStoreBuilder.name(), topicName.value)
    assert(helper.materialized != null)
  }
}
