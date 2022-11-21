package com.github.chenharryhua.nanjin.spark.listeners

import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import org.apache.spark.sql.streaming.StreamingQueryListener
import fs2.Stream
import fs2.concurrent.Channel
import org.apache.spark.sql.SparkSession

final private class SparkSStreamListener[F[_]](
  bus: Channel[F, StreamingQueryListener.Event],
  dispatcher: Dispatcher[F])
    extends StreamingQueryListener {
  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit =
    dispatcher.unsafeRunAndForget(bus.send(event))
}

object SparkSStreamListener {
  def apply[F[_]](ss: SparkSession)(implicit F: Async[F]): Stream[F, StreamingQueryListener.Event] =
    for {
      bus <- Stream.eval(Channel.unbounded[F, StreamingQueryListener.Event])
      dispatcher <- Stream.resource(Dispatcher.sequential[F])
      _ <- Stream.bracket {
        F.blocking {
          val listener = new SparkSStreamListener(bus, dispatcher)
          ss.streams.addListener(listener)
          listener
        }
      }(listener => F.blocking(ss.streams.removeListener(listener)))
      event <- bus.stream
    } yield event
}
