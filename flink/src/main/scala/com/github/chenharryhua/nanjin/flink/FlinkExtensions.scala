package com.github.chenharryhua.nanjin.flink

import com.github.chenharryhua.nanjin.codec.NJConsumerRecord
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord

private[flink] trait FlinkExtensions extends Serializable {

  implicit final class FlinKafkaExtension[F[_], K, V](topic: => KafkaTopic[F, K, V])
      extends Serializable {

    def dataStream(env: StreamExecutionEnvironment)(
      implicit kf: TypeInformation[K],
      vf: TypeInformation[V]): DataStream[NJConsumerRecord[K, V]] =
      env.addSource[NJConsumerRecord[K, V]](
        new FlinkKafkaConsumer[NJConsumerRecord[K, V]](
          topic.topicDef.topicName,
          new KafkaDeserializationSchema[NJConsumerRecord[K, V]] {
            override def isEndOfStream(nextElement: NJConsumerRecord[K, V]): Boolean = false

            override def getProducedType: TypeInformation[NJConsumerRecord[K, V]] =
              implicitly[TypeInformation[NJConsumerRecord[K, V]]]

            override def deserialize(
              record: ConsumerRecord[Array[Byte], Array[Byte]]): NJConsumerRecord[K, V] =
              topic.decoder(record).record
          },
          topic.context.settings.consumerSettings.consumerProperties))
  }
}
