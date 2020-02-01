package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Concurrent
import frameless.TypedEncoder
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.OutputMode

final class SparkStreaming[F[_], A: TypedEncoder](ds: Dataset[A]) extends Serializable {

  def transform[B: TypedEncoder](f: Dataset[A] => Dataset[B]) =
    new SparkStreaming[F, B](f(ds))

  def run(implicit F: Concurrent[F]): F[Unit] = {
    val ss = ds.writeStream.outputMode(OutputMode.Update).format("console")
    F.bracket(F.delay(ss.start))(s => F.delay(s.awaitTermination()))(_ => F.pure(()))
  }
}
