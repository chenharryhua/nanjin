package com.github.chenharryhua.nanjin.spark.dstream

import cats.data.Kleisli
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Dispatcher
import fs2.Stream
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.concurrent.duration.FiniteDuration

final class DStreamRunner[F[_]] private (
  sparkContext: SparkContext,
  checkpoint: String,
  batchDuration: Duration,
  streamings: List[Kleisli[F, StreamingContext, DStreamRunner.Mark]])(implicit F: Async[F])
    extends Serializable {

  def signup[A](rd: Kleisli[F, StreamingContext, A])(f: A => DStreamRunner.Mark): DStreamRunner[F] =
    new DStreamRunner[F](sparkContext, checkpoint, batchDuration, streamings :+ rd.map(f))

  private def createContext(dispatcher: Dispatcher[F])(): StreamingContext = {
    val ssc = new StreamingContext(sparkContext, batchDuration)
    streamings.foreach(ksd => dispatcher.unsafeRunSync(ksd.run(ssc)))
    ssc.checkpoint(checkpoint)
    ssc
  }

  /** non-terminating function
    */
  def run: F[Nothing] = {
    val exec: Resource[F, Unit] = Dispatcher[F].evalMap { dispatcher =>
      F.bracket(F.blocking {
        val ssc: StreamingContext = StreamingContext.getOrCreate(checkpoint, createContext(dispatcher))
        ssc.start()
        ssc
      })(ssc => F.interruptible(many = false)(ssc.awaitTermination()))(ssc =>
        F.blocking(ssc.stop(stopSparkContext = false, stopGracefully = true)))
    }
    exec.use[Nothing](_ => F.never)
  }

  def stream: Stream[F, Nothing] = Stream.eval(run)
}

object DStreamRunner {
  private[dstream] object Mark
  type Mark = Mark.type

  def apply[F[_]: Async](
    sparkContext: SparkContext,
    checkpoint: String,
    batchDuration: FiniteDuration): DStreamRunner[F] =
    new DStreamRunner[F](sparkContext, checkpoint, Seconds(batchDuration.toSeconds), Nil)
}
