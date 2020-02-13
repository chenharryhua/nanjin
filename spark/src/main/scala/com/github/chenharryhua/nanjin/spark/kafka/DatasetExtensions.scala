package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import org.apache.spark.sql.SparkSession

private[kafka] trait DatasetExtensions {

  implicit final class SparKafkaTopicSyntax[K, V](kit: KafkaTopicKit[K, V]) extends Serializable {

    def sparKafka(params: ConfigParamF.ConfigParam)(implicit spark: SparkSession): FsmStart[K, V] =
      new FsmStart(kit, params)

    def sparKafka(implicit spark: SparkSession): FsmStart[K, V] =
      sparKafka(ConfigParamF.defaultParams)
  }
}
