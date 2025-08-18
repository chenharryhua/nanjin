package com.github.chenharryhua.nanjin.guard.service

import cats.Eval
import cats.effect.kernel.{Ref, Sync}
import cats.implicits.{
  catsSyntaxEq,
  catsSyntaxIfM,
  catsSyntaxPartialOrder,
  toFlatMapOps,
  toFunctorOps,
  toTraverseOps
}
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.{
  MetricReport,
  MetricReset,
  ServiceMessage,
  ServicePanic,
  ServiceStart,
  ServiceStop
}
import com.github.chenharryhua.nanjin.guard.event.{Error, Event, ServiceStopCause}
import com.github.chenharryhua.nanjin.guard.translator.{jsonHelper, ColorScheme, Translator}
import io.circe.Encoder
import org.typelevel.log4cats.MessageLogger

import scala.io.AnsiColor

final private class EventLogger[F[_]](
  translator: Translator[F, String],
  logger: MessageLogger[F],
  alarmLevel: Ref[F, Option[AlarmLevel]])(implicit F: Sync[F]) {

  private def transform_event(event: Event): F[Option[String]] =
    translator
      .translate(event)
      .map(_.map { text =>
        val title: String = ColorScheme
          .decorate(event)
          .run {
            case ColorScheme.GoodColor =>
              Eval.now(AnsiColor.GREEN + event.name.entryName + AnsiColor.RESET)
            case ColorScheme.InfoColor =>
              Eval.now(AnsiColor.CYAN + event.name.entryName + AnsiColor.RESET)
            case ColorScheme.WarnColor =>
              Eval.now(AnsiColor.YELLOW + event.name.entryName + AnsiColor.RESET)
            case ColorScheme.ErrorColor =>
              Eval.now(AnsiColor.RED + event.name.entryName + AnsiColor.RESET)
          }
          .value
        s"$title $text"
      })

  /*
   * compulsory events
   */
  def service_start(ss: ServiceStart): F[Unit] =
    transform_event(ss).flatMap(_.traverse(logger.info(_))).void

  def service_stop(ss: ServiceStop): F[Unit] =
    ss.cause match {
      case ServiceStopCause.Successfully =>
        transform_event(ss).flatMap(_.traverse(logger.info(_))).void
      case ServiceStopCause.Maintenance =>
        transform_event(ss).flatMap(_.traverse(logger.info(_))).void
      case ServiceStopCause.ByCancellation =>
        transform_event(ss).flatMap(_.traverse(logger.warn(_))).void
      case ServiceStopCause.ByException(_) =>
        transform_event(ss).flatMap(_.traverse(logger.error(_))).void
    }

  def service_panic(ss: ServicePanic): F[Unit] =
    transform_event(ss).flatMap(_.traverse(logger.error(_))).void

  def metric_report(ss: MetricReport): F[Unit] =
    ColorScheme.decorate(ss).run(Eval.now).value match {
      case ColorScheme.GoodColor =>
        transform_event(ss).flatMap(_.traverse(logger.info(_))).void
      case ColorScheme.InfoColor =>
        transform_event(ss).flatMap(_.traverse(logger.info(_))).void
      case ColorScheme.WarnColor =>
        transform_event(ss).flatMap(_.traverse(logger.warn(_))).void
      case ColorScheme.ErrorColor =>
        transform_event(ss).flatMap(_.traverse(logger.error(_))).void
    }

  def metric_reset(ss: MetricReset): F[Unit] =
    ColorScheme.decorate(ss).run(Eval.now).value match {
      case ColorScheme.GoodColor =>
        transform_event(ss).flatMap(_.traverse(logger.info(_))).void
      case ColorScheme.InfoColor =>
        transform_event(ss).flatMap(_.traverse(logger.info(_))).void
      case ColorScheme.WarnColor =>
        transform_event(ss).flatMap(_.traverse(logger.warn(_))).void
      case ColorScheme.ErrorColor =>
        transform_event(ss).flatMap(_.traverse(logger.error(_))).void
    }

  /*
   * service message
   */
  private def transform_service_message[S: Encoder](
    serviceParams: ServiceParams,
    msg: S,
    level: AlarmLevel,
    error: Option[Error]): F[Option[String]] =
    alarmLevel.get
      .map(_.exists(_ <= level))
      .ifM(create_service_message(serviceParams, msg, level, error).flatMap(transform_event(_)), F.pure(None))

  def info[S: Encoder](serviceParams: ServiceParams, msg: S): F[Unit] =
    transform_service_message(serviceParams, msg, AlarmLevel.Info, None)
      .flatMap(_.traverse(logger.info(_)))
      .void

  def done[S: Encoder](serviceParams: ServiceParams, msg: S): F[Unit] =
    transform_service_message(serviceParams, msg, AlarmLevel.Done, None)
      .flatMap(_.traverse(logger.info(_)))
      .void

  def warn[S: Encoder](serviceParams: ServiceParams, msg: S): F[Unit] =
    transform_service_message(serviceParams, msg, AlarmLevel.Warn, None)
      .flatMap(_.traverse(logger.warn(_)))
      .void

  def warn[S: Encoder](ex: Throwable)(serviceParams: ServiceParams, msg: S): F[Unit] =
    transform_service_message(serviceParams, msg, AlarmLevel.Warn, Some(Error(ex)))
      .flatMap(_.traverse(logger.warn(_)))
      .void

  def error[S: Encoder](serviceParams: ServiceParams, msg: S): F[Unit] =
    transform_service_message(serviceParams, msg, AlarmLevel.Error, None)
      .flatMap(_.traverse(logger.error(_)))
      .void

  def error[S: Encoder](ex: Throwable)(serviceParams: ServiceParams, msg: S): F[Unit] =
    transform_service_message(serviceParams, msg, AlarmLevel.Error, Some(Error(ex)))
      .flatMap(_.traverse(logger.error(_)))
      .void

  def debug[S: Encoder](serviceParams: ServiceParams, msg: => F[S]): F[Unit] = {
    def debug_message(ss: ServiceMessage): String = {
      val title = AnsiColor.BLUE + ss.name.entryName + AnsiColor.RESET
      val txt = jsonHelper.json_service_message(ss).noSpaces
      s"$title $txt"
    }
    alarmLevel.get
      .map(_.exists(_ === AlarmLevel.Debug))
      .ifM(
        F.attempt(msg).flatMap {
          case Left(ex) =>
            create_service_message(serviceParams, "Error Message", AlarmLevel.Debug, Some(Error(ex)))
              .flatMap(m => logger.debug(debug_message(m)))
          case Right(value) =>
            create_service_message(serviceParams, value, AlarmLevel.Debug, None)
              .flatMap(m => logger.debug(debug_message(m)))
        },
        F.unit
      )
  }

  def debug[S: Encoder](serviceParams: ServiceParams, msg: S): F[Unit] =
    debug(serviceParams, F.pure(msg))

  val void: F[Unit] = F.unit
}
