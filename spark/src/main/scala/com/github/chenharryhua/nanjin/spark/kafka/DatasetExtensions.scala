package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import org.apache.spark.sql.SparkSession

import java.time.ZoneId

private[kafka] trait DatasetExtensions {

  implicit final class SparKafkaTopicSyntax[F[_], K, V](topic: KafkaTopic[F, K, V])
      extends Serializable {

    def sparKafka(cfg: SKConfig)(implicit ss: SparkSession): SparKafka[F, K, V] =
      new SparKafka(topic, ss, cfg)

    def sparKafka(zoneId: ZoneId)(implicit ss: SparkSession): SparKafka[F, K, V] =
      sparKafka(SKConfig(topic.topicDef.topicName, zoneId))

    def sparKafka(dtr: NJDateTimeRange)(implicit ss: SparkSession): SparKafka[F, K, V] =
      sparKafka(SKConfig(topic.topicDef.topicName, dtr))

    def sparKafka(implicit ss: SparkSession): SparKafka[F, K, V] =
      sparKafka(SKConfig(topic.topicDef.topicName, ZoneId.systemDefault()))

  }

}
