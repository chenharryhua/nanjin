package com.github.chenharryhua.nanjin.flink.kafka

import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import org.apache.flink.api.common.typeinfo.TypeInformation

private[kafka] trait FlinKafkaExtensions extends Serializable {

  implicit final class FlinKafkaExtension[F[_], K: TypeInformation, V: TypeInformation](
    kit: KafkaTopicKit[F, K, V])
      extends Serializable {

    def flinKafka                          = new FlinKafkaSession[F, K, V](kit, FlinKafkaParams.default)
    def flinKafka(params: FlinKafkaParams) = new FlinKafkaSession[F, K, V](kit, params)

  }
}
