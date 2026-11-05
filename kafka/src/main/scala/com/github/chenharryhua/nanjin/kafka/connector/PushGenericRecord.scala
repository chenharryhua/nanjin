package com.github.chenharryhua.nanjin.kafka.connector

import com.github.chenharryhua.nanjin.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.config.SerdeSettings
import fs2.kafka.ProducerRecord
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.GenericRecord

import scala.jdk.CollectionConverters.given

/** Encodes the key and value of an Avro `GenericRecord` into raw Kafka bytes for producing.
  *
  * The inverse of `PullGenericRecord`. Both the key and value are serialized with a Confluent
  * `KafkaAvroSerializer`, which registers/looks up each side's schema in `srClient` and emits the Confluent
  * wire format (a 1-byte `0x00` magic byte plus a 4-byte schema id, then the Avro payload). This applies
  * uniformly to record and primitive schema types alike. `null` values serialize to `null`.
  */
final private class PushGenericRecord(
  srClient: SchemaRegistryClient,
  serdeSettings: SerdeSettings,
  topicName: TopicName) {

  private val topic: String = topicName.value

  /** Build a value-to-byte encoder backed by a Confluent `KafkaAvroSerializer`. `isKey` selects key vs value
    * serializer configuration (and hence the `-key`/`-value` subject). The serializer emits the Confluent
    * wire format; `null` encodes to `null`.
    */
  private def getEncoder(isKey: Boolean): AnyRef => Array[Byte] = {
    val ser = new KafkaAvroSerializer(srClient)
    ser.configure(serdeSettings.properties.asJava, isKey)
    (data: AnyRef) => ser.serialize(topic, data)
  }

  private val key_serialize: AnyRef => Array[Byte] = getEncoder(true)
  private val val_serialize: AnyRef => Array[Byte] = getEncoder(false)

  /** Extract the `key` and `value` fields from `gr`, serialize each to bytes, and build a byte
    * `ProducerRecord` targeting the configured topic.
    *
    * @param gr
    *   a `GenericRecord` shaped like `NJConsumerRecord` (its `key`/`value` fields are encoded).
    */
  def fromGenericRecord(gr: GenericRecord): ProducerRecord[Array[Byte], Array[Byte]] =
    ProducerRecord(topic, key_serialize(gr.get("key")), val_serialize(gr.get("value")))
}
