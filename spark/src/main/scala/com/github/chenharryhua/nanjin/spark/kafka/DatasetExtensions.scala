package com.github.chenharryhua.nanjin.spark.kafka

import java.time.ZoneId

import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import org.apache.spark.sql.SparkSession

private[kafka] trait DatasetExtensions {

  implicit final class SparKafkaTopicSyntax[F[_], K, V](topic: KafkaTopic[F, K, V])
      extends Serializable {

    def sparKafka(cfg: SKConfig)(implicit spark: SparkSession): SparKafka[F, K, V] =
      new SparKafka(topic, cfg)

    def sparKafka(zoneId: ZoneId)(implicit spark: SparkSession): SparKafka[F, K, V] =
      sparKafka(SKConfig(zoneId))

    def sparKafka(dtr: NJDateTimeRange)(implicit spark: SparkSession): SparKafka[F, K, V] =
      sparKafka(SKConfig(dtr))

    def sparKafka(implicit spark: SparkSession): SparKafka[F, K, V] =
      sparKafka(SKConfig(ZoneId.systemDefault()))

  }
}
