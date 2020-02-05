package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.Concurrent
import com.github.chenharryhua.nanjin.utils.Keyboard
import org.apache.spark.sql.streaming.DataStreamWriter
import fs2.Stream

final class SparkStreamRunner[F[_], A](dsw: DataStreamWriter[A], sink: StreamOutputSink)
    extends Serializable {

  def withOptions(f: DataStreamWriter[A] => DataStreamWriter[A]) =
    new SparkStreamRunner[F, A](f(sink.sinkOptions(dsw)), sink)

  def partitionBy(colNames: String*): SparkStreamRunner[F, A] =
    new SparkStreamRunner[F, A](dsw.partitionBy(colNames: _*), sink)

  def run(implicit F: Concurrent[F]): F[Unit] = {
    val q = for {
      signal <- Keyboard.signal[F]
      streamQuery <- Stream(dsw.start())

    } yield ()

    F.bracket(F.delay(dsw.start))(s => F.delay(s.awaitTermination()))(s => F.pure(s.stop()))
  }
}
