package com.github.chenharryhua.nanjin.spark.listeners

import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import fs2.concurrent.Channel
import fs2.Stream
import org.apache.spark.streaming.scheduler.*
import org.apache.spark.streaming.StreamingContext

final private class SparkDStreamListener[F[_]](
  bus: Channel[F, StreamingListenerEvent],
  dispatcher: Dispatcher[F])
    extends StreamingListener {

  override def onStreamingStarted(event: StreamingListenerStreamingStarted): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onReceiverStarted(event: StreamingListenerReceiverStarted): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onReceiverError(event: StreamingListenerReceiverError): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onReceiverStopped(event: StreamingListenerReceiverStopped): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onBatchSubmitted(event: StreamingListenerBatchSubmitted): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onBatchStarted(event: StreamingListenerBatchStarted): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onBatchCompleted(event: StreamingListenerBatchCompleted): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onOutputOperationStarted(event: StreamingListenerOutputOperationStarted): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onOutputOperationCompleted(event: StreamingListenerOutputOperationCompleted): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

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
