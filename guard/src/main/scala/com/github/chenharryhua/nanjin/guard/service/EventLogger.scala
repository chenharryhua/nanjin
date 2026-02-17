package com.github.chenharryhua.nanjin.guard.service

import cats.Eval
import cats.effect.kernel.{Ref, Sync}
import cats.effect.std.Console
import cats.syntax.applicative.catsSyntaxApplicativeId
import cats.syntax.functor.toFunctorOps
import cats.syntax.flatMap.{catsSyntaxIfM, toFlatMapOps}
import cats.syntax.eq.catsSyntaxEq
import cats.syntax.order.catsSyntaxPartialOrder
import cats.syntax.traverse.toTraverseOps
import com.github.chenharryhua.nanjin.guard.config.LogFormat.Console_Json_MultiLine
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, Domain, LogFormat, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.{
  MetricsReport,
  MetricsReset,
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
  val domain: Domain,
  alarmLevel: Ref[F, Option[AlarmLevel]],
  translator: Translator[F, String],
  logger: MessageLogger[F],
  isColoring: Boolean)(implicit F: Sync[F])
    extends Log[F] {

  def withDomain(domain: Domain): EventLogger[F] =
    new EventLogger[F](serviceParams, domain, alarmLevel, translator, logger, isColoring)

  private def transform_event(event: Event): F[Option[String]] =
    translator
      .translate(event)
      .map(_.map { text =>
        val name: String = event.name.entryName
        val title: String =
          if (isColoring) {
            ColorScheme
              .decorate(event)
              .run {
                case ColorScheme.GoodColor  => Eval.now(AnsiColor.GREEN + name + AnsiColor.RESET)
                case ColorScheme.InfoColor  => Eval.now(AnsiColor.CYAN + name + AnsiColor.RESET)
                case ColorScheme.WarnColor  => Eval.now(AnsiColor.YELLOW + name + AnsiColor.RESET)
                case ColorScheme.ErrorColor => Eval.now(AnsiColor.RED + name + AnsiColor.RESET)
              }
              .value
          } else name

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

  def metrics_report(ss: MetricsReport): F[Unit] =
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

  def metrics_reset(ss: MetricsReset): F[Unit] =
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

  def logServiceMessage(sm: ServiceMessage): F[Option[Unit]] =
    transform_event(sm).flatMap(_.traverse(txt =>
      sm.level match {
        case AlarmLevel.Error => logger.error(txt)
        case AlarmLevel.Warn  => logger.warn(txt)
        case AlarmLevel.Done  => logger.info(txt)
        case AlarmLevel.Info  => logger.info(txt)
        case AlarmLevel.Debug => logger.debug(txt)
      }))

  /*
   * Log
   */

  private def sm2text[S: Encoder](msg: S, level: AlarmLevel, error: Option[Error]): F[Option[String]] =
    alarmLevel.get
      .map(_.exists(_ <= level))
      .ifM(
        service_message[F, S](serviceParams, domain, msg, level, error).flatMap(transform_event(_)),
        F.pure(None))

  override def info[S: Encoder](msg: S): F[Unit] =
    sm2text(msg, AlarmLevel.Info, None).flatMap(_.traverse(logger.info(_))).void

  override def done[S: Encoder](msg: S): F[Unit] =
    sm2text(msg, AlarmLevel.Done, None).flatMap(_.traverse(logger.info(_))).void

  override def warn[S: Encoder](msg: S): F[Unit] =
    sm2text(msg, AlarmLevel.Warn, None).flatMap(_.traverse(logger.warn(_))).void

  override def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    sm2text(msg, AlarmLevel.Warn, Some(Error(ex))).flatMap(_.traverse(logger.warn(_))).void

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
            service_message[F, String](
              serviceParams,
              domain,
              "Error Message",
              AlarmLevel.Debug,
              Some(Error(ex))).flatMap(m => logger.debug(debug_message(m)))
          case Right(value) =>
            service_message[F, S](serviceParams, domain, value, AlarmLevel.Debug, None)
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
    domain: Domain,
    alarmLevel: Ref[F, Option[AlarmLevel]]): F[EventLogger[F]] =
    serviceParams.logFormat match {
      /*
       * console
       */
      case LogFormat.Console_PlainText =>
        new EventLogger[F](
          serviceParams = serviceParams,
          domain = domain,
          translator = SimpleTextTranslator[F],
          logger = new ConsoleLogger[F](serviceParams.zoneId),
          alarmLevel = alarmLevel,
          isColoring = true
        ).pure
      case LogFormat.Console_Json_OneLine =>
        new EventLogger[F](
          serviceParams = serviceParams,
          domain = domain,
          translator = PrettyJsonTranslator[F].map(_.noSpaces),
          logger = new ConsoleLogger[F](serviceParams.zoneId),
          alarmLevel = alarmLevel,
          isColoring = true
        ).pure
      case Console_Json_MultiLine =>
        new EventLogger[F](
          serviceParams = serviceParams,
          domain = domain,
          translator = PrettyJsonTranslator[F].map(_.spaces2),
          logger = new ConsoleLogger[F](serviceParams.zoneId),
          alarmLevel = alarmLevel,
          isColoring = true
        ).pure
      case LogFormat.Console_JsonVerbose =>
        new EventLogger[F](
          serviceParams = serviceParams,
          domain = domain,
          translator = Translator.idTranslator.map(_.asJson.spaces2),
          logger = new ConsoleLogger[F](serviceParams.zoneId),
          alarmLevel = alarmLevel,
          isColoring = true
        ).pure

      /*
       * slf4j
       */
      case LogFormat.Slf4j_PlainText =>
        Slf4jLogger
          .fromName[F](serviceParams.serviceName.value)
          .map(logger =>
            new EventLogger[F](
              serviceParams = serviceParams,
              domain = domain,
              translator = SimpleTextTranslator[F],
              logger = logger,
              alarmLevel = alarmLevel,
              isColoring = false))
      case LogFormat.Slf4j_Json_OneLine =>
        Slf4jLogger
          .fromName[F](serviceParams.serviceName.value)
          .map(logger =>
            new EventLogger[F](
              serviceParams = serviceParams,
              domain = domain,
              translator = PrettyJsonTranslator[F].map(_.noSpaces),
              logger = logger,
              alarmLevel = alarmLevel,
              isColoring = false))
      case LogFormat.Slf4j_Json_MultiLine =>
        Slf4jLogger
          .fromName[F](serviceParams.serviceName.value)
          .map(logger =>
            new EventLogger[F](
              serviceParams = serviceParams,
              domain = domain,
              translator = PrettyJsonTranslator[F].map(_.spaces2),
              logger = logger,
              alarmLevel = alarmLevel,
              isColoring = false))
    }
}
