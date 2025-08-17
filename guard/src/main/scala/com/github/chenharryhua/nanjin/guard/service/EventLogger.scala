package com.github.chenharryhua.nanjin.guard.service

import cats.implicits.{toFlatMapOps, toFunctorOps, toTraverseOps}
import cats.{Eval, Monad}
import com.github.chenharryhua.nanjin.guard.config.AlarmLevel
import com.github.chenharryhua.nanjin.guard.event.Event.{
  MetricReport,
  MetricReset,
  ServiceMessage,
  ServicePanic,
  ServiceStart,
  ServiceStop
}
import com.github.chenharryhua.nanjin.guard.event.{Event, ServiceStopCause}
import com.github.chenharryhua.nanjin.guard.translator.{ColorScheme, Translator}
import org.typelevel.log4cats.Logger

final private class EventLogger[F[_]: Monad](translator: Translator[F, String], logger: Logger[F]) {
  private def color_event(event: Event): F[Option[String]] =
    translator
      .translate(event)
      .map(_.map { text =>
        val title: String = ColorScheme
          .decorate(event)
          .run {
            case ColorScheme.GoodColor  => Eval.now(Console.GREEN + event.name.entryName + Console.RESET)
            case ColorScheme.InfoColor  => Eval.now(Console.CYAN + event.name.entryName + Console.RESET)
            case ColorScheme.WarnColor  => Eval.now(Console.YELLOW + event.name.entryName + Console.RESET)
            case ColorScheme.ErrorColor => Eval.now(Console.RED + event.name.entryName + Console.RESET)
          }
          .value
        s"$title $text"
      })

  def service_start(ss: ServiceStart): F[Unit] =
    color_event(ss).flatMap(_.traverse(logger.info(_))).void
  def service_stop(ss: ServiceStop): F[Unit] =
    ss.cause match {
      case ServiceStopCause.Successfully =>
        color_event(ss).flatMap(_.traverse(logger.info(_))).void
      case ServiceStopCause.Maintenance =>
        color_event(ss).flatMap(_.traverse(logger.info(_))).void
      case ServiceStopCause.ByCancellation =>
        color_event(ss).flatMap(_.traverse(logger.warn(_))).void
      case ServiceStopCause.ByException(_) =>
        color_event(ss).flatMap(_.traverse(logger.error(_))).void
    }

  def service_panic(ss: ServicePanic): F[Unit] =
    color_event(ss).flatMap(_.traverse(logger.error(_))).void

  def metric_report(ss: MetricReport): F[Unit] =
    color_event(ss).flatMap(_.traverse(logger.info(_))).void

  def metric_reset(ss: MetricReset): F[Unit] =
    color_event(ss).flatMap(_.traverse(logger.info(_))).void

  def service_message(ss: ServiceMessage): F[Unit] =
    color_event(ss)
      .flatMap(_.traverse(m =>
        ss.level match {
          case AlarmLevel.Error => logger.error(m)
          case AlarmLevel.Warn  => logger.warn(m)
          case AlarmLevel.Done  => logger.info(m)
          case AlarmLevel.Info  => logger.info(m)
          case AlarmLevel.Debug => logger.debug(m)
        }))
      .void
}
