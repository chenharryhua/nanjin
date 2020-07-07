package com.github.chenharryhua.nanjin.spark.kafka

import java.time.ZoneId

import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import org.apache.spark.sql.SparkSession

private[kafka] trait DatasetExtensions {

  implicit final class SparKafkaTopicSyntax[F[_], K, V](topic: KafkaTopic[F, K, V])
      extends Serializable {

    def sparKafka(cfg: SKConfig)(implicit spark: SparkSession): FsmStart[F, K, V] =
      new FsmStart(topic, cfg)

    def sparKafka(zoneId: ZoneId)(implicit spark: SparkSession): FsmStart[F, K, V] =
      new FsmStart(topic, SKConfig(zoneId))

    def sparKafka(dtr: NJDateTimeRange)(implicit spark: SparkSession): FsmStart[F, K, V] =
      new FsmStart(topic, SKConfig(dtr))

  }
}
