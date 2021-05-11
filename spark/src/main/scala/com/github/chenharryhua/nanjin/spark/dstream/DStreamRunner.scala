package com.github.chenharryhua.nanjin.spark.dstream

import cats.Applicative
import cats.data.Kleisli
import cats.effect.Async
import cats.effect.std.Dispatcher
import cats.syntax.traverse._
import fs2.Stream
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.concurrent.duration.FiniteDuration

final class EndMark private {}

private[dstream] object EndMark {
  val mark: EndMark = new EndMark
}

final class DStreamRunner[F[_]: Applicative] private (
  sparkContext: SparkContext,
  checkpoint: String,
  batchDuration: Duration,
  streamings: List[Kleisli[F, StreamingContext, EndMark]])
    extends Serializable {

  def signup[A](rd: Kleisli[F, StreamingContext, A])(f: A => EndMark): DStreamRunner[F] =
    new DStreamRunner[F](sparkContext, checkpoint, batchDuration, streamings :+ rd.map(f))

  private def createContext(dispatcher: Dispatcher[F])(): StreamingContext = {
    val ssc      = new StreamingContext(sparkContext, batchDuration)
    val register = streamings.traverse(_(ssc))
    dispatcher.unsafeRunSync(register)
    ssc.checkpoint(checkpoint)
    ssc
  }

  def run(implicit F: Async[F]): Stream[F, Unit] = {
    val start = for {
      dispatcher <- Stream.resource(Dispatcher[F])
      ssc <- Stream.bracket(F.delay {
        val ssc: StreamingContext = StreamingContext.getOrCreate(checkpoint, createContext(dispatcher))
        ssc.start()
        ssc
      })(ssc => F.delay(ssc.stop(stopSparkContext = false, stopGracefully = true)))
    } yield ()
    start ++ Stream.never[F]
  }
}

object DStreamRunner {

  def apply[F[_]: Applicative](
    sparkContext: SparkContext,
    checkpoint: String,
    batchDuration: FiniteDuration): DStreamRunner[F] =
    new DStreamRunner[F](sparkContext, checkpoint, Seconds(batchDuration.toSeconds), Nil)
}
