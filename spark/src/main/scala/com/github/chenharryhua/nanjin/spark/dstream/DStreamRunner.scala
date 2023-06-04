package com.github.chenharryhua.nanjin.spark.dstream

import cats.data.Kleisli
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.concurrent.duration.FiniteDuration

@annotation.nowarn
final class DStreamRunner[F[_]] private (
  sparkContext: SparkContext,
  checkpoint: NJPath,
  batchDuration: Duration,
  streamings: List[Kleisli[F, StreamingContext, DStreamRunner.Mark]],
  freshStart: Boolean // true: delete checkpoint before start, false: keep checkpoint
)(implicit F: Async[F])
    extends Serializable {

  def signup[A](rd: Kleisli[F, StreamingContext, A])(f: A => DStreamRunner.Mark): DStreamRunner[F] =
    new DStreamRunner[F](sparkContext, checkpoint, batchDuration, rd.map(f) :: streamings, freshStart)

  /** delete checkpoint before start the dstream
    */
  def withFreshStart: DStreamRunner[F] =
    new DStreamRunner[F](sparkContext, checkpoint, batchDuration, streamings, freshStart = true)

  private def createContext(dispatcher: Dispatcher[F])(): StreamingContext = {
    val ssc = new StreamingContext(sparkContext, batchDuration)
    streamings.foreach(ksd => dispatcher.unsafeRunSync(ksd.run(ssc)))
    ssc.checkpoint(checkpoint.pathStr)
    ssc
  }

  val resource: Resource[F, StreamingContext] =
    for {
      dispatcher <- Dispatcher.sequential[F]
      _ <- Resource.eval(NJHadoop[F](sparkContext.hadoopConfiguration).delete(checkpoint).whenA(freshStart))
      sc <- Resource
        .make(F.blocking(StreamingContext.getOrCreate(checkpoint.pathStr, createContext(dispatcher))))(ssc =>
          F.blocking(ssc.stop(stopSparkContext = false, stopGracefully = true)))
        .evalMap(ssc => F.blocking(ssc.start()).as(ssc))
    } yield sc

}

object DStreamRunner {
  private[dstream] object Mark
  type Mark = Mark.type

  def apply[F[_]: Async](
    sparkContext: SparkContext,
    checkpoint: NJPath,
    batchDuration: FiniteDuration): DStreamRunner[F] =
    new DStreamRunner[F](sparkContext, checkpoint, Seconds(batchDuration.toSeconds), Nil, false)
}
