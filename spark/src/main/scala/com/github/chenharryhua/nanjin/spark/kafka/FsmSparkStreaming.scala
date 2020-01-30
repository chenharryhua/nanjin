package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.OutputMode

final class FsmSparkStreaming[F[_], A](
  ds: Dataset[A],
  params: SparKafkaParams
) extends FsmSparKafka {

  def transform[B](f: Dataset[A] => Dataset[B]) =
    new FsmSparkStreaming[F, B](f(ds), params)

  def run(implicit F: Sync[F]): F[Unit] = {
    val ss = ds.writeStream.outputMode(OutputMode.Update).format("console")
    F.bracket(F.delay(ss.start))(s => F.delay(s.awaitTermination()))(_ => F.pure(()))
  }
}
