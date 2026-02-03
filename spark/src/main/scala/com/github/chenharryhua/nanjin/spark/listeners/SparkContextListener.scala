package com.github.chenharryhua.nanjin.spark.listeners

import cats.Functor
import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import cats.syntax.functor.*
import fs2.Stream
import fs2.concurrent.Channel
import org.apache.spark.scheduler.*
import org.apache.spark.{SparkContext, SparkFirehoseListener}

final private class SparkContextListener[F[_]: Functor](
  bus: Channel[F, SparkListenerEvent],
  dispatcher: Dispatcher[F])
    extends SparkFirehoseListener {
  override def onEvent(event: SparkListenerEvent): Unit =
    dispatcher.unsafeRunSync(bus.send(event).void)
}

object SparkContextListener {
  def apply[F[_]](sparkContext: SparkContext)(implicit F: Async[F]): Stream[F, SparkListenerEvent] =
    for {
      bus <- Stream.eval(Channel.unbounded[F, SparkListenerEvent])
      dispatcher <- Stream.resource(Dispatcher.sequential[F])
      _ <- Stream.bracket {
        F.blocking {
          val listener = new SparkContextListener(bus, dispatcher)
          sparkContext.addSparkListener(listener)
          listener
        }
      }(listener => F.blocking(sparkContext.removeSparkListener(listener)))
      event <- bus.stream
    } yield event

}
