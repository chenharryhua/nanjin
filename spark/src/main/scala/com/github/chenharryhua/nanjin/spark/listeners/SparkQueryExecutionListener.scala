package com.github.chenharryhua.nanjin.spark.listeners

import cats.Functor
import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import cats.syntax.functor.toFunctorOps
import fs2.Stream
import fs2.concurrent.Channel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

sealed trait QueryExecutionEvent
object QueryExecutionEvent {
  final case class SuccessEvent(funcName: String, qe: QueryExecution, durationNs: Long)
      extends QueryExecutionEvent
  final case class FailureEvent(funcName: String, qe: QueryExecution, exception: Exception)
      extends QueryExecutionEvent
}

final private class SparkQueryExecutionListener[F[_]: Functor](
  bus: Channel[F, QueryExecutionEvent],
  dispatcher: Dispatcher[F])
    extends QueryExecutionListener {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    dispatcher.unsafeRunSync(bus.send(QueryExecutionEvent.SuccessEvent(funcName, qe, durationNs)).void)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
    dispatcher.unsafeRunSync(bus.send(QueryExecutionEvent.FailureEvent(funcName, qe, exception)).void)
}

object SparkQueryExecutionListener {
  def apply[F[_]](ss: SparkSession)(implicit F: Async[F]): Stream[F, QueryExecutionEvent] =
    for {
      bus <- Stream.eval(Channel.unbounded[F, QueryExecutionEvent])
      dispatcher <- Stream.resource(Dispatcher.sequential[F])
      _ <- Stream.bracket {
        F.blocking {
          val listener = new SparkQueryExecutionListener(bus, dispatcher)
          ss.listenerManager.register(listener)
          listener
        }
      }(listener => F.blocking(ss.listenerManager.unregister(listener)))
      event <- bus.stream
    } yield event
}
