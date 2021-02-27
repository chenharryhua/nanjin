package com.github.chenharryhua.nanjin.spark.dstream

import cats.Functor
import cats.data.Reader
import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import cats.syntax.all._
import fs2.{INothing, Stream}
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
  streamings: List[Reader[StreamingContext, F[EndMark]]])
    extends Serializable {

  def probe[A](rd: Reader[StreamingContext, F[A]])(f: A => EndMark)(implicit F: Functor[F]): DStreamRunner[F] =
    new DStreamRunner[F](sparkContext, checkpoint, batchDuration, streamings :+ rd.map(_.map(f)))

  private def createContext(dispatcher: Dispatcher[F])(): StreamingContext = {
    val ssc = new StreamingContext(sparkContext, batchDuration)
    streamings.foreach(f => dispatcher.unsafeRunSync(f(ssc)))
    ssc.checkpoint(checkpoint)
    ssc
  }

  def run(implicit F: Async[F]): Stream[F, INothing] = {
    val ss = for {
      dispatcher <- Stream.resource(Dispatcher[F])
      _ <- Stream.bracket(F.blocking {
        val ssc: StreamingContext = StreamingContext.getOrCreate(checkpoint, createContext(dispatcher))
        ssc.start()
        ssc
      })(ssc => F.blocking(ssc.stop(stopSparkContext = false, stopGracefully = true)))
    } yield ()
    ss >> Stream.never[F]
  }
}

object DStreamRunner {

  def apply[F[_]](sparkContext: SparkContext, checkpoint: String, batchDuration: FiniteDuration): DStreamRunner[F] =
    new DStreamRunner[F](sparkContext, checkpoint, Seconds(batchDuration.toSeconds), Nil)
}
