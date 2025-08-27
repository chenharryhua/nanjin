package com.github.chenharryhua.nanjin.guard.service

import cats.Eval
import cats.effect.kernel.{Ref, Sync}
import cats.effect.std.Console
import cats.implicits.{
  catsSyntaxApplicativeId,
  catsSyntaxEq,
  catsSyntaxIfM,
  catsSyntaxPartialOrder,
  toFlatMapOps,
  toFunctorOps,
  toTraverseOps
}
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, LogFormat, ServiceParams}
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
import io.circe.syntax.EncoderOps
import org.typelevel.log4cats.MessageLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.io.AnsiColor

sealed trait Log[F[_]] {

  def debug[S: Encoder](msg: S): F[Unit]
  def debug[S: Encoder](msg: => F[S]): F[Unit]

  def info[S: Encoder](msg: S): F[Unit]
  def done[S: Encoder](msg: S): F[Unit]

  def warn[S: Encoder](msg: S): F[Unit]
  def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit]

  def void[S](msg: S): F[Unit]
}

final private class EventLogger[F[_]](
  val serviceParams: ServiceParams,
  translator: Translator[F, String],
  logger: MessageLogger[F],
  alarmLevel: Ref[F, Option[AlarmLevel]])(implicit F: Sync[F])
    extends Log[F] {

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
    msg: S,
    level: AlarmLevel,
    error: Option[Error]): F[Option[String]] =
    alarmLevel.get
      .map(_.exists(_ <= level))
      .ifM(serviceMessage(serviceParams, msg, level, error).flatMap(transform_event(_)), F.pure(None))

  def error[S: Encoder](msg: S): F[Unit] =
    transform_service_message(msg, AlarmLevel.Error, None).flatMap(_.traverse(logger.error(_))).void

  def error[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    transform_service_message(msg, AlarmLevel.Error, Some(Error(ex)))
      .flatMap(_.traverse(logger.error(_)))
      .void

  override def info[S: Encoder](msg: S): F[Unit] =
    transform_service_message(msg, AlarmLevel.Info, None).flatMap(_.traverse(logger.info(_))).void

  override def done[S: Encoder](msg: S): F[Unit] =
    transform_service_message(msg, AlarmLevel.Done, None).flatMap(_.traverse(logger.info(_))).void

  override def warn[S: Encoder](msg: S): F[Unit] =
    transform_service_message(msg, AlarmLevel.Warn, None).flatMap(_.traverse(logger.warn(_))).void

  override def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    transform_service_message(msg, AlarmLevel.Warn, Some(Error(ex))).flatMap(_.traverse(logger.warn(_))).void

  override def debug[S: Encoder](msg: => F[S]): F[Unit] = {
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
            serviceMessage(serviceParams, "Error Message", AlarmLevel.Debug, Some(Error(ex)))
              .flatMap(m => logger.debug(debug_message(m)))
          case Right(value) =>
            serviceMessage(serviceParams, value, AlarmLevel.Debug, None)
              .flatMap(m => logger.debug(debug_message(m)))
        },
        F.unit
      )
  }

  override def debug[S: Encoder](msg: S): F[Unit] =
    debug(F.pure(msg))

  override def void[S](msg: S): F[Unit] = F.unit
}

private object EventLogger {
  def apply[F[_]: Sync: Console](
    serviceParams: ServiceParams,
    alarmLevel: Ref[F, Option[AlarmLevel]]): F[EventLogger[F]] =
    serviceParams.logFormat match {
      case LogFormat.Console_PlainText =>
        new EventLogger[F](
          serviceParams,
          SimpleTextTranslator[F],
          new ConsoleLogger[F](serviceParams.zoneId),
          alarmLevel).pure
      case LogFormat.Console_JsonNoSpaces =>
        new EventLogger[F](
          serviceParams,
          PrettyJsonTranslator[F].map(_.noSpaces),
          new ConsoleLogger[F](serviceParams.zoneId),
          alarmLevel).pure
      case LogFormat.Slf4j_PlainText =>
        Slf4jLogger
          .fromName[F](serviceParams.serviceName.value)
          .map(logger => new EventLogger[F](serviceParams, SimpleTextTranslator[F], logger, alarmLevel))
      case LogFormat.Slf4j_JsonNoSpaces =>
        Slf4jLogger
          .fromName[F](serviceParams.serviceName.value)
          .map(logger =>
            new EventLogger[F](serviceParams, PrettyJsonTranslator[F].map(_.noSpaces), logger, alarmLevel))
      case LogFormat.Slf4j_JsonSpaces2 =>
        Slf4jLogger
          .fromName[F](serviceParams.serviceName.value)
          .map(logger =>
            new EventLogger[F](serviceParams, PrettyJsonTranslator[F].map(_.spaces2), logger, alarmLevel))
      case LogFormat.Slf4j_JsonVerbose =>
        Slf4jLogger
          .fromName[F](serviceParams.serviceName.value)
          .map(logger =>
            new EventLogger[F](
              serviceParams,
              Translator.idTranslator.map(_.asJson.spaces2),
              logger,
              alarmLevel))
    }
}
