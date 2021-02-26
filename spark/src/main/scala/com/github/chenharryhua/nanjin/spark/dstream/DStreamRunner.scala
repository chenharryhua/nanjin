package com.github.chenharryhua.nanjin.spark.dstream

import cats.Monad
import cats.data.Reader
import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import fs2.Stream
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import cats.syntax.all._

import scala.concurrent.duration.FiniteDuration

final class EndMark private {}

private[dstream] object EndMark {
  val mark: EndMark = new EndMark
}

final class DStreamRunner[F[_]] private (
  sparkContext: SparkContext,
  checkpoint: String,
  batchDuration: Duration,
  streamings: List[Reader[StreamingContext, F[EndMark]]])
    extends Serializable {

  def signup[A](rd: Reader[StreamingContext, F[A]])(f: A => EndMark)(implicit F: Monad[F]): DStreamRunner[F] =
    new DStreamRunner[F](sparkContext, checkpoint, batchDuration, streamings :+ rd.map(_.map(f)))

  private def createContext(dispatcher: Dispatcher[F])(): StreamingContext = {
    val ssc = new StreamingContext(sparkContext, batchDuration)
    streamings.foreach(f => dispatcher.unsafeRunSync(f(ssc)))
    ssc.checkpoint(checkpoint)
    ssc
  }

  def run(implicit F: Async[F]): Stream[F, Unit] =
    for {
      dispatcher <- Stream.resource(Dispatcher[F])
      _ <- Stream
        .bracket(F.delay {
          val ssc: StreamingContext = StreamingContext.getOrCreate(checkpoint, createContext(dispatcher))
          ssc.start()
          ssc
        })(ssc => F.delay(ssc.stop(stopSparkContext = false, stopGracefully = false)))
        .evalMap(ssc => F.delay(ssc.awaitTermination()))
    } yield ()
}

object DStreamRunner {

  def apply[F[_]](sparkContext: SparkContext, checkpoint: String, batchDuration: FiniteDuration): DStreamRunner[F] =
    new DStreamRunner[F](sparkContext, checkpoint, Seconds(batchDuration.toSeconds), Nil)
}
