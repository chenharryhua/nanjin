package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import com.github.chenharryhua.nanjin.kafka.KafkaTopicDescription
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final class FsmSparkStreaming[F[_], K: TypedEncoder, V: TypedEncoder](
  ds: Dataset[NJConsumerRecord[K, V]],
  initState: FsmInit[K, V]
) extends FsmSparKafka {

  def run(implicit F: Sync[F]): F[Unit] = ???
  // F.bracket(F.delay(streamSet.start))(s => F.delay(s.awaitTermination()))(_ => F.pure(()))

}
