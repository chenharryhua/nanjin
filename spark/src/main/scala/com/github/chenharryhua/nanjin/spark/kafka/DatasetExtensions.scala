package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import org.apache.spark.sql.SparkSession

private[kafka] trait DatasetExtensions {

  implicit final class SparKafkaTopicSyntax[F[_], K, V](kit: KafkaTopicKit[F, K, V])
      extends Serializable {

    def sparKafka(cfg: SKConfig)(implicit spark: SparkSession): FsmStart[F, K, V] =
      new FsmStart(kit, cfg)

    def sparKafka(implicit spark: SparkSession): FsmStart[F, K, V] =
      sparKafka(SKConfig.defaultConfig)
  }
}
