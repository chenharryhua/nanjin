package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Ref, Sync}
import cats.effect.std.Console
import cats.syntax.applicative.catsSyntaxApplicativeId
import cats.syntax.eq.catsSyntaxEq
import cats.syntax.flatMap.{catsSyntaxIfM, toFlatMapOps}
import cats.syntax.functor.toFunctorOps
import cats.syntax.order.catsSyntaxPartialOrder
import cats.syntax.traverse.toTraverseOps
import com.github.chenharryhua.nanjin.guard.config.LogFormat.Console_Json_MultiLine
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, LogFormat, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.{Domain, Event, StackTrace}
import com.github.chenharryhua.nanjin.guard.translator.{
  eventTitle,
  Attribute,
  ColorScheme,
  PrettyJsonTranslator,
  SimpleTextTranslator,
  Translator
}
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.typelevel.log4cats.MessageLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.io.AnsiColor

// error must go through herald
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

  private def green(name: String): String =
    if (isColoring) AnsiColor.GREEN + name + AnsiColor.RESET else name
  private def cyan(name: String): String =
    if (isColoring) AnsiColor.CYAN + name + AnsiColor.RESET else name
  private def yellow(name: String): String =
    if (isColoring) AnsiColor.YELLOW + name + AnsiColor.RESET else name
  private def red(name: String): String =
    if (isColoring) AnsiColor.RED + name + AnsiColor.RESET else name

  def logEvent(event: Event): F[Unit] =
    translator
      .translate(event)
      .flatMap(_.traverse { text =>
        ColorScheme.decorate[F, Unit](event).run {
          case ColorScheme.GoodColor  => logger.info(s"${green(eventTitle(event))} $text")
          case ColorScheme.InfoColor  => logger.info(s"${cyan(eventTitle(event))} $text")
          case ColorScheme.WarnColor  => logger.warn(s"${yellow(eventTitle(event))} $text")
          case ColorScheme.ErrorColor => logger.error(s"${red(eventTitle(event))} $text")
        }
      })
      .void

  /*
   * Log
   */

  private def log_service_message[S: Encoder](
    msg: S,
    level: AlarmLevel,
    stackTrace: Option[StackTrace]): F[Unit] =
    alarmLevel.get
      .map(_.exists(_ <= level))
      .ifM(
        create_service_message[F, S](serviceParams, domain, msg, level, stackTrace).flatMap(logEvent),
        F.unit)

  override def info[S: Encoder](msg: S): F[Unit] = log_service_message(msg, AlarmLevel.Info, None)
  override def done[S: Encoder](msg: S): F[Unit] = log_service_message(msg, AlarmLevel.Done, None)
  override def warn[S: Encoder](msg: S): F[Unit] = log_service_message(msg, AlarmLevel.Warn, None)
  override def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit] =
    log_service_message(msg, AlarmLevel.Warn, Some(StackTrace(ex)))

  /*
   * debug
   */
  private val debug_title: String =
    if (isColoring) AnsiColor.BLUE + "Debug Message" + AnsiColor.RESET else "Debug Message"
  private val debug_service_name = Attribute(serviceParams.serviceName).camelJsonEntry
  private val debug_domain_name = Attribute(domain).camelJsonEntry
  override def debug[S: Encoder](msg: => F[S]): F[Unit] = {
    def debug_message(message: Json, stackTrace: Option[StackTrace]): String = {
      val txt: String = // service_id and correlation are irrelevant in debug
        Json
          .obj(
            debug_service_name,
            debug_domain_name,
            "message" -> message,
            Attribute(stackTrace).camelJsonEntry
          )
          .dropNullValues
          .noSpaces

      s"$debug_title $txt"
    }

    alarmLevel.get
      .map(_.exists(_ === AlarmLevel.Debug))
      .ifM(
        F.attempt(msg).flatMap {
          case Left(ex)     => logger.debug(debug_message("Debug Error".asJson, Some(StackTrace(ex))))
          case Right(value) => logger.debug(debug_message(value.asJson, None))
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
