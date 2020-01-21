package com.github.chenharryhua.nanjin.flink

import com.github.chenharryhua.nanjin.kafka.{KafkaTopicDescription, NJConsumerRecord}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord

private[flink] trait FlinkExtensions extends Serializable {

  implicit final class FlinKafkaExtension[K: TypeInformation, V: TypeInformation](
    description: KafkaTopicDescription[K, V])
      extends Serializable {

    def flinKafka = new FlinKafkaSession[K, V](description, FlinKafkaParams.default)
  }
}
