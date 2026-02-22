package com.github.chenharryhua.nanjin.guard.logging

import cats.effect.kernel.Sync
import cats.effect.std.AtomicCell
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.option.catsSyntaxOptionId
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceMessage
import com.github.chenharryhua.nanjin.guard.event.{Domain, Event, StackTrace}
import fs2.concurrent.Channel
import io.circe.Encoder
import org.apache.commons.collections4.queue.CircularFifoQueue

object Herald {
  def apply[F[_]: Sync](
    serviceParams: ServiceParams,
    domain: Domain,
    channel: Channel[F, Event],
    errorHistory: AtomicCell[F, CircularFifoQueue[ServiceMessage]]): Log[F] =
    new HeraldImpl[F](serviceParams, domain, channel, errorHistory)

  final private class HeraldImpl[F[_]](
    serviceParams: ServiceParams,
    domain: Domain,
    channel: Channel[F, Event],
    errorHistory: AtomicCell[F, CircularFifoQueue[ServiceMessage]])(implicit F: Sync[F])
      extends Log[F] {

    private def create[S: Encoder](
      msg: S,
      level: AlarmLevel,
      stackTrace: Option[StackTrace]): F[ServiceMessage] =
      create_service_message[F, S](
        serviceParams = serviceParams,
        domain = domain,
        msg = msg,
        level = level,
        stackTrace = stackTrace)

    override def info[S: Encoder](msg: S): F[Unit] =
      for {
        evt <- create(msg, AlarmLevel.Info, None)
        _ <- channel.send(evt)
      } yield ()

    override def done[S: Encoder](msg: S): F[Unit] =
      for {
        evt <- create(msg, AlarmLevel.Done, None)
        _ <- channel.send(evt)
      } yield ()

    override def warn[S: Encoder](msg: S): F[Unit] =
      for {
        evt <- create(msg, AlarmLevel.Warn, None)
        _ <- channel.send(evt)
      } yield ()

    override def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
      for {
        evt <- create(msg, AlarmLevel.Warn, Some(StackTrace(ex)))
        _ <- channel.send(evt)
      } yield ()

    override def error[S: Encoder](msg: S): F[Unit] =
      for {
        evt <- create(msg, AlarmLevel.Error, None)
        _ <- errorHistory.modify(queue => (queue, queue.add(evt)))
        _ <- channel.send(evt)
      } yield ()

    override def error[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
      for {
        evt <- create(msg, AlarmLevel.Error, StackTrace(ex).some)
        _ <- errorHistory.modify(queue => (queue, queue.add(evt)))
        _ <- channel.send(evt)
      } yield ()

    override def debug[S: Encoder](msg: S): F[Unit] = F.unit
    override def debug[S: Encoder](msg: => F[S]): F[Unit] = F.unit
    override def void[S](msg: S): F[Unit] = F.unit
  }
}
