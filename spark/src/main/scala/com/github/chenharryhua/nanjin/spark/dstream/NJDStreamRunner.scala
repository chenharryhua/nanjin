package com.github.chenharryhua.nanjin.spark.dstream

import cats.effect.Sync
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}

final class NJDStreamRunner(sparkSession: SparkSession, checkpoint: String, duration: Duration) {

  def run[F[_]](f: StreamingContext => Unit)(implicit F: Sync[F]): F[Unit] =
    F.bracket(F.delay {
      val ctx: StreamingContext = new StreamingContext(sparkSession.sparkContext, duration)
      ctx.checkpoint(checkpoint)
      f(ctx)
      ctx.start()
      ctx
    })(c => F.delay(c.awaitTermination()))(c =>
      F.delay(c.stop(stopSparkContext = false, stopGracefully = true)))
}
