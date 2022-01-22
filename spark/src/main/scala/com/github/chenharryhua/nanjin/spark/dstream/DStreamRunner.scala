package com.github.chenharryhua.nanjin.spark.dstream

import cats.data.Kleisli
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import fs2.Stream
import fs2.concurrent.Channel
import org.apache.spark.SparkContext
import org.apache.spark.streaming.scheduler.*
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.concurrent.duration.FiniteDuration

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

  private class Listener(dispatcher: Dispatcher[F], bus: Channel[F, StreamingListenerEvent]) extends StreamingListener {

    override def onStreamingStarted(event: StreamingListenerStreamingStarted): Unit =
      dispatcher.unsafeRunSync(bus.send(event).void)

    override def onReceiverStarted(event: StreamingListenerReceiverStarted): Unit =
      dispatcher.unsafeRunSync(bus.send(event).void)

    override def onReceiverError(event: StreamingListenerReceiverError): Unit =
      dispatcher.unsafeRunSync(bus.send(event).void)

    override def onReceiverStopped(event: StreamingListenerReceiverStopped): Unit =
      dispatcher.unsafeRunSync(bus.send(event).void)

    override def onBatchSubmitted(event: StreamingListenerBatchSubmitted): Unit =
      dispatcher.unsafeRunSync(bus.send(event).void)

    override def onBatchStarted(event: StreamingListenerBatchStarted): Unit =
      dispatcher.unsafeRunSync(bus.send(event).void)

    override def onBatchCompleted(event: StreamingListenerBatchCompleted): Unit =
      dispatcher.unsafeRunSync(bus.send(event).void)

    override def onOutputOperationStarted(event: StreamingListenerOutputOperationStarted): Unit =
      dispatcher.unsafeRunSync(bus.send(event).void)

    override def onOutputOperationCompleted(event: StreamingListenerOutputOperationCompleted): Unit =
      dispatcher.unsafeRunSync(bus.send(event).void)

  }

  private val resource: Resource[F, StreamingContext] =
    for {
      dispatcher <- Dispatcher[F]
      _ <- Resource.eval(NJHadoop[F](sparkContext.hadoopConfiguration).delete(checkpoint).whenA(freshStart))
      sc <- Resource
        .make(F.blocking(StreamingContext.getOrCreate(checkpoint.pathStr, createContext(dispatcher))))(ssc =>
          F.blocking(ssc.stop(stopSparkContext = false, stopGracefully = true)))
        .evalMap(ssc => F.blocking(ssc.start()).as(ssc))
    } yield sc

  def stream: Stream[F, StreamingListenerEvent] = for {
    dispatcher <- Stream.resource(Dispatcher[F])
    ssc <- Stream.resource(resource)
    event <- Stream.eval(Channel.unbounded[F, StreamingListenerEvent]).flatMap { bus =>
      ssc.addStreamingListener(new Listener(dispatcher, bus))
      bus.stream
    }
  } yield event
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
