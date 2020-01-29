package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.kafka.KafkaTopicDescription
import org.apache.spark.sql.SparkSession

private[kafka] trait DatasetExtensions {

  implicit final class SparKafkaTopicSyntax[K, V](description: KafkaTopicDescription[K, V])
      extends Serializable {

    def sparKafka(implicit spark: SparkSession): FsmInit[K, V] =
      new FsmInit(description, SparKafkaParams.default)
  }
}
