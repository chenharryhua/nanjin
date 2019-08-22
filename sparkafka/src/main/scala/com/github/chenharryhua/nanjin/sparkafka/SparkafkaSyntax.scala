package com.github.chenharryhua.nanjin.sparkafka

import java.time.LocalDateTime

import cats.Monad
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession

trait SparkafkaSyntax {
  implicit final class KafkaTopicDatasetOps[F[_], K, V](
    private val topic: SharedVariable[KafkaTopic[F, K, V]]) {

    def dataset(start: LocalDateTime, end: LocalDateTime)(
      implicit spark: SparkSession,
      ev1: TypedEncoder[K],
      ev2: TypedEncoder[V],
      ev3: Monad[F]): F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
      SparkafkaDataset.sdataset(topic, start, end)

    //def valueSet(start: LocalDateTime, end: LocalDateTime) = ???
  }
}
