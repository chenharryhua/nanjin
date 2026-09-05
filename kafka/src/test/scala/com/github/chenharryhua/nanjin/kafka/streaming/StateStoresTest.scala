package com.github.chenharryhua.nanjin.kafka.streaming

import com.github.chenharryhua.nanjin.kafka.{TopicName, TopicSerde}
import com.github.chenharryhua.nanjin.kafka.serdes.KafkaSerde
import org.apache.kafka.common.serialization.Serdes
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration

class StateStoresTest extends AnyFunSuite {

  private val topicName: TopicName = TopicName("state.store.test")

  private val topic: TopicSerde[String, String] =
    TopicSerde(
      topicName,
      new KafkaSerde[String](Serdes.String(), topicName),
      new KafkaSerde[String](Serdes.String(), topicName))

  private val stores: StateStores[String, String] = StateStores(topic)

  test("1.store name is the topic name") {
    assert(stores.name === topicName.value)
  }

  test("2.persistent key-value store supplier carries the store name and builders are available") {
    val helper = stores.persistentKeyValueStore
    assert(helper.supplier.name() === topicName.value)
    assert(helper.keyValueStoreBuilder.name() === topicName.value)
    assert(helper.timestampedKeyValueStoreBuilder.name() === topicName.value)
    assert(helper.materialized != null)
  }

  test("3.in-memory and lru key-value stores are available") {
    assert(stores.inMemoryKeyValueStore.supplier.name() === topicName.value)
    assert(stores.persistentTimestampedKeyValueStore.supplier.name() === topicName.value)
    assert(stores.lruMap(100).supplier.name() === topicName.value)
  }

  test("4.window stores build with the given retention and window size") {
    val helper =
      stores.persistentWindowStore(Duration.ofHours(1), Duration.ofMinutes(5), retainDuplicates = false)
    assert(helper.supplier.name() === topicName.value)
    assert(helper.windowStoreBuilder.name() === topicName.value)
    assert(helper.timestampedWindowStoreBuilder.name() === topicName.value)
    // in-memory and timestamped variants are also available
    assert(
      stores.inMemoryWindowStore(
        Duration.ofHours(1),
        Duration.ofMinutes(5),
        false).supplier.name() === topicName.value)
    assert(
      stores.persistentTimestampedWindowStore(Duration.ofHours(1), Duration.ofMinutes(5), false)
        .supplier.name() === topicName.value)
  }

  test("5.session stores build with the given retention") {
    assert(stores.persistentSessionStore(Duration.ofHours(1)).sessionStoreBuilder.name() === topicName.value)
    assert(stores.inMemorySessionStore(Duration.ofHours(1)).supplier.name() === topicName.value)
  }

  test("6.query parameters carry the store name") {
    assert(stores.queries.keyValueStore.storeName() === topicName.value)
    assert(stores.queries.timestampedKeyValueStore.storeName() === topicName.value)
    assert(stores.queries.windowStore.storeName() === topicName.value)
    assert(stores.queries.timestampedWindowStore.storeName() === topicName.value)
    assert(stores.queries.sessionStore.storeName() === topicName.value)
  }
}
