package com.github.chenharryhua.nanjin.flink.kafka

import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import org.apache.flink.api.common.typeinfo.TypeInformation

private[kafka] trait FlinKafkaExtensions extends Serializable {

  implicit final class FlinKafkaExtension[K: TypeInformation, V: TypeInformation](
    kit: KafkaTopicKit[K, V])
      extends Serializable {

    def flinKafka                          = new FlinKafkaSession[K, V](kit, FlinKafkaParams.default)
    def flinKafka(params: FlinKafkaParams) = new FlinKafkaSession[K, V](kit, params)

  }
}
