package com.github.chenharryhua.nanjin.spark.dstream

import cats.data.Reader
import cats.effect.kernel.Async
import fs2.Stream
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.concurrent.duration.FiniteDuration

final class EndMark private {}

private[dstream] object EndMark {
  val mark: EndMark = new EndMark
}

final class DStreamRunner[F[_]] private (
  sparkContext: SparkContext,
  checkpoint: String,
  batchDuration: Duration,
  streamings: List[Reader[StreamingContext, EndMark]])
    extends Serializable {

  def signup[A](rd: Reader[StreamingContext, A])(f: A => EndMark): DStreamRunner[F] =
    new DStreamRunner[F](sparkContext, checkpoint, batchDuration, streamings :+ rd.map(f))

  private def createContext(): StreamingContext = {
    val ssc = new StreamingContext(sparkContext, batchDuration)
    streamings.foreach(_(ssc))
    ssc.checkpoint(checkpoint)
    ssc
  }

  def run(implicit F: Async[F]): Stream[F, Unit] =
    Stream
      .bracket(F.delay {
        val ssc: StreamingContext = StreamingContext.getOrCreate(checkpoint, createContext)
        ssc.start()
        ssc
      })(ssc => F.delay(ssc.stop(stopSparkContext = false, stopGracefully = false)))
      .evalMap(ssc => F.delay(ssc.awaitTermination()))
}

object DStreamRunner {

  def apply[F[_]](sparkContext: SparkContext, checkpoint: String, batchDuration: FiniteDuration): DStreamRunner[F] =
    new DStreamRunner[F](sparkContext, checkpoint, Seconds(batchDuration.toSeconds), Nil)
}
