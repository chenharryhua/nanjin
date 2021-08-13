package com.github.chenharryhua.nanjin.guard.alert

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.{Pipe, Stream}
import org.log4s.Logger

object showLog {
  private[this] val logger: Logger = org.log4s.getLogger

  def pipe[F[_]](implicit F: Sync[F]): Pipe[F, NJEvent, Unit] = { (events: Stream[F, NJEvent]) =>
    events.evalMap { event =>
      val out: String = event.show
      event match {
        case ServicePanic(_, _, _, _, error)    => F.blocking(logger.warn(error.throwable)(out))
        case ActionRetrying(_, _, _, _, error)  => F.blocking(logger.warn(error.throwable)(out))
        case ActionFailed(_, _, _, _, _, error) => F.blocking(logger.error(error.throwable)(out))
        case _                                  => F.blocking(logger.info(out))
      }
    }
  }
}
