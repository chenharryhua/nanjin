package com.github.chenharryhua.nanjin.spark.listeners

import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import cats.Functor
import cats.syntax.functor.*
import fs2.concurrent.Channel
import fs2.Stream
import org.apache.spark.streaming.scheduler.*
import org.apache.spark.streaming.StreamingContext

final private class SparkDStreamListener[F[_]: Functor](
  bus: Channel[F, StreamingListenerEvent],
  dispatcher: Dispatcher[F])
    extends StreamingListener {

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

object SparkDStreamListener {
  def apply[F[_]](sc: StreamingContext)(implicit F: Async[F]): Stream[F, StreamingListenerEvent] =
    for {
      bus <- Stream.eval(Channel.unbounded[F, StreamingListenerEvent])
      dispatcher <- Stream.resource(Dispatcher.sequential[F])
      _ <- Stream.bracket {
        F.blocking {
          val listener = new SparkDStreamListener(bus, dispatcher)
          sc.addStreamingListener(listener)
          listener
        }
      }(listener => F.blocking(sc.removeStreamingListener(listener)))
      event <- bus.stream
    } yield event
}
