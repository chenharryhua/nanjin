package com.github.chenharryhua.nanjin.flink.kafka

import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.kafka.KafkaTopicDescription
import com.github.chenharryhua.nanjin.kafka.data.NJConsumerRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord

final class FlinKafkaSession[K: TypeInformation, V: TypeInformation](
  description: KafkaTopicDescription[K, V],
  params: FlinKafkaParams)
    extends Serializable with UpdateParams[FlinKafkaParams, FlinKafkaSession[K, V]] {

  override def withParamUpdate(f: FlinKafkaParams => FlinKafkaParams): FlinKafkaSession[K, V] =
    new FlinKafkaSession[K, V](description, f(params))

  def dataStream: DataStream[NJConsumerRecord[K, V]] =
    params.env.addSource[NJConsumerRecord[K, V]](
      new FlinkKafkaConsumer[NJConsumerRecord[K, V]](
        description.topicDef.topicName.value,
        new KafkaDeserializationSchema[NJConsumerRecord[K, V]] {
          override def isEndOfStream(nextElement: NJConsumerRecord[K, V]): Boolean = false

          override def getProducedType: TypeInformation[NJConsumerRecord[K, V]] =
            implicitly[TypeInformation[NJConsumerRecord[K, V]]]

          override def deserialize(
            record: ConsumerRecord[Array[Byte], Array[Byte]]): NJConsumerRecord[K, V] =
            description.decoder(record).record
        },
        description.settings.consumerSettings.consumerProperties))
}
