package com.github.chenharryhua.nanjin.flink

import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord

private[flink] trait FlinkExtensions extends Serializable {

  implicit final class FlinKafkaExtension[F[_], K, V](topic: => KafkaTopic[F, K, V])
      extends Serializable {

    def dataStream(env: StreamExecutionEnvironment)(
      implicit ev: TypeInformation[V]): DataStream[V] =
      env.addSource[V](
        new FlinkKafkaConsumer[V](topic.topicDef.topicName, new KafkaDeserializationSchema[V] {
          override def isEndOfStream(nextElement: V): Boolean = false
          override def getProducedType: TypeInformation[V]    = ev

          override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): V =
            topic.decoder(record).decode.value
        }, topic.context.settings.consumerSettings.consumerProperties))
  }
}
