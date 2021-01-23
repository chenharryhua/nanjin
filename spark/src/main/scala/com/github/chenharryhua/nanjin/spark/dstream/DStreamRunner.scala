package com.github.chenharryhua.nanjin.spark.dstream

import cats.data.Reader
import cats.effect.Sync
import fs2.Stream
import monocle.Getter
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.concurrent.duration.FiniteDuration

final class EndMark private {}

private[dstream] object EndMark {
  val mark: EndMark = new EndMark
}

final class DStreamRunner[F[_]] private (sc: StreamingContext, checkpoint: String) extends Serializable {

  def signup[A](rd: Reader[StreamingContext, A])(f: A => EndMark): this.type = {
    f(rd.run(sc))
    this
  }

  def dstream[A, B](rd: Reader[StreamingContext, A])(implicit getter: Getter[A, DStream[B]]): DStream[B] =
    getter.get(rd.run(sc))

  def run(implicit F: Sync[F]): Stream[F, Unit] =
    Stream.bracket(F.delay {
      sc.checkpoint(checkpoint)
      sc.start()
      sc.awaitTermination()
    })(_ => F.delay(sc.stop(stopSparkContext = false, stopGracefully = true)))
}

object DStreamRunner {

  def apply[F[_]](sparkContext: SparkContext, checkpoint: String, batchDuration: FiniteDuration): DStreamRunner[F] = {
    val sc = StreamingContext.getOrCreate(
      checkpoint,
      () => new StreamingContext(sparkContext, Seconds(batchDuration.toSeconds)))
    new DStreamRunner[F](sc, checkpoint)
  }
}
