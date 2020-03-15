package com.github.chenharryhua.nanjin.flink.kafka

import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import org.apache.flink.api.common.typeinfo.TypeInformation

private[kafka] trait FlinKafkaExtensions extends Serializable {

  implicit final class FlinKafkaExtension[F[_], K: TypeInformation, V: TypeInformation](
    topic: KafkaTopic[F, K, V])
      extends Serializable {

    def flinKafka                          = new FlinKafkaSession[F, K, V](topic, FlinKafkaParams.default)
    def flinKafka(params: FlinKafkaParams) = new FlinKafkaSession[F, K, V](topic, params)

  }
}
