package com.github.chenharryhua.nanjin.flink.kafka

import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJConsumerRecordDecoder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord

final class FlinKafkaSession[F[_], K: TypeInformation, V: TypeInformation](
  topic: KafkaTopic[F, K, V],
  params: FlinKafkaParams)
    extends Serializable with UpdateParams[FlinKafkaParams, FlinKafkaSession[F, K, V]] {

  override def withParamUpdate(f: FlinKafkaParams => FlinKafkaParams): FlinKafkaSession[F, K, V] =
    new FlinKafkaSession[F, K, V](topic, f(params))

  def dataStream: DataStream[OptionalKV[K, V]] =
    params.env.addSource[OptionalKV[K, V]](
      new FlinkKafkaConsumer[OptionalKV[K, V]](
        topic.topicDef.topicName.value,
        new KafkaDeserializationSchema[OptionalKV[K, V]] {

          override def isEndOfStream(nextElement: OptionalKV[K, V]): Boolean = false

          override def getProducedType: TypeInformation[OptionalKV[K, V]] =
            implicitly[TypeInformation[OptionalKV[K, V]]]

          override def deserialize(
            record: ConsumerRecord[Array[Byte], Array[Byte]]): OptionalKV[K, V] =
            topic.njDecoder.decode(record).run._2
        },
        topic.settings.consumerSettings.javaProperties))
}
