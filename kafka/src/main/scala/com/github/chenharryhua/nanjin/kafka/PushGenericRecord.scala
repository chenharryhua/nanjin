package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import org.apache.avro.Schema

final class PushGenericRecord(
  topicName: TopicName,
  keySchema: Schema,
  valSchema: Schema,
  srs: SchemaRegistrySettings) extends Serializable {


}
