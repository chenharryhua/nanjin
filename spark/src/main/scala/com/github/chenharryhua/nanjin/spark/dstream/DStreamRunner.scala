package com.github.chenharryhua.nanjin.spark.dstream

import cats.data.Kleisli
import cats.effect.Async
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

  def run: Stream[F, Unit] = {
    val start = for {
      dispatcher <- Stream.resource(Dispatcher[F])
      _ <- Stream
        .bracket(F.blocking {
          val ssc: StreamingContext = StreamingContext.getOrCreate(checkpoint, createContext(dispatcher))
          ssc.start()
          dispatcher.unsafeRunAndForget(F.blocking(ssc.awaitTermination()))
          ssc
        })(ssc => F.blocking(ssc.stop(stopSparkContext = false, stopGracefully = true)))
        .evalMap(ssc => F.interruptible(many = false)(ssc.awaitTermination()))
    } yield ()
    start ++ Stream.never[F]
  }
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
