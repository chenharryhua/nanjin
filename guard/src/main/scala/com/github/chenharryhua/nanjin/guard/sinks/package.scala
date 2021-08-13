package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.Sync
import cats.effect.std.Console
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.{ActionFailed, ActionRetrying, NJEvent, ServicePanic}
import fs2.{INothing, Pipe, Stream}
import io.circe.syntax.*
import org.log4s.Logger

package object sinks {
  def jsonConsole[F[_]](implicit C: Console[F]): Pipe[F, NJEvent, INothing] =
    _.evalMap(event => C.println(event.asJson)).drain

  private[this] val logger: Logger = org.log4s.getLogger

  def showLog[F[_]](implicit F: Sync[F]): Pipe[F, NJEvent, INothing] = { (events: Stream[F, NJEvent]) =>
    events.evalMap { event =>
      val out: String = event.show
      event match {
        case ServicePanic(_, _, _, _, error)    => F.blocking(logger.warn(error.throwable)(out))
        case ActionRetrying(_, _, _, _, error)  => F.blocking(logger.warn(error.throwable)(out))
        case ActionFailed(_, _, _, _, _, error) => F.blocking(logger.error(error.throwable)(out))
        case _                                  => F.blocking(logger.info(out))
      }
    }.drain
  }

}
