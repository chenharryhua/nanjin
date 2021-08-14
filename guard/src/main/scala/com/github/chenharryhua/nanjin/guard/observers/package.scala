package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.Sync
import cats.effect.std.Console
import cats.syntax.all.*
import com.codahale.metrics.ConsoleReporter
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.{INothing, Pipe, Stream}
import io.circe.syntax.*
import org.log4s.Logger

package object observers {

  def jsonConsole[F[_]: Console]: Pipe[F, NJEvent, INothing] =
    _.evalMap(event => Console[F].println(event.asJson)).drain

  def showConsole[F[_]: Console]: Pipe[F, NJEvent, INothing] =
    _.evalMap(event => Console[F].println(event.show)).drain

  private[this] val logger: Logger = org.log4s.getLogger

  private def logging[F[_]](f: NJEvent => String)(implicit F: Sync[F]): Pipe[F, NJEvent, INothing] = {
    (events: Stream[F, NJEvent]) =>
      events.evalMap { event =>
        val out: String = f(event)
        event match {
          case ServicePanic(_, _, _, _, error) =>
            F.blocking(error.throwable.fold(logger.error(out))(ex => logger.error(ex)(out)))
          case ActionRetrying(_, _, _, _, error) =>
            F.blocking(error.throwable.fold(logger.warn(out))(ex => logger.warn(ex)(out)))
          case ActionFailed(_, _, _, _, _, error) =>
            F.blocking(error.throwable.fold(logger.error(out))(ex => logger.error(ex)(out)))
          case _ => F.blocking(logger.info(out))
        }
      }.drain
  }

  def showLog[F[_]: Sync]: Pipe[F, NJEvent, INothing] = logging[F](_.show)
  def jsonLog[F[_]: Sync]: Pipe[F, NJEvent, INothing] = logging[F](_.asJson.noSpaces)

  def metricConsole[F[_]](builder: ConsoleReporter.Builder => ConsoleReporter.Builder)(implicit
    F: Sync[F]): Pipe[F, NJEvent, INothing] = { (events: Stream[F, NJEvent]) =>
    events.collect { case MetricsReport(_, _, _, MetricRegistryWrapper(Some(mr))) => mr }.evalMap { mr =>
      F.delay(builder(ConsoleReporter.forRegistry(mr)).build().report())
    }.drain
  }
  def metricConsole[F[_]](implicit F: Sync[F]): Pipe[F, NJEvent, INothing] =
    metricConsole[F]((b: ConsoleReporter.Builder) => b)

}
