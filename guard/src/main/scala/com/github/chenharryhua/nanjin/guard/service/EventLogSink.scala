package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Sync
import cats.effect.std.Console
import cats.implicits.showInterpolator
import cats.syntax.applicative.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import cats.{Defer, Monad}
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.config.{LogFormat, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.service.LogSink
import com.github.chenharryhua.nanjin.guard.translator.{
  eventLogLevel,
  eventTitle,
  PrettyJsonTranslator,
  SimpleTextTranslator,
  Translator
}
import io.circe.syntax.EncoderOps
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{LoggerName, MessageLogger}

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

private object EventLogSink:
  def apply[F[_]: {Console, Sync}](serviceParams: ServiceParams): LogSink[F] =
    serviceParams.logFormat match {
      case Some(format) =>
        eventLogSink[F](logFormat = format, loggerName = LoggerName(serviceParams.serviceName.value))
      case None => LogSink(_ => ().pure[F])
    }

  private def slf4j[F[_]: {Monad, Defer}](
    logger: MessageLogger[F],
    translator: Translator[F, String],
    colorMode: ColorMode) =
    LogSink { (event: Event) =>
      translator
        .translate(event)
        .flatMap(_.traverse { text =>
          val title = eventTitle(event)
          eventLogLevel[F, Unit](event).run {
            case LogLevel.Debug => logger.debug(s"${colorMode.debug(title)} $text")
            case LogLevel.Info  => logger.info(s"${colorMode.info(title)} $text")
            case LogLevel.Good  => logger.info(s"${colorMode.good(title)} $text")
            case LogLevel.Warn  => logger.warn(s"${colorMode.warn(title)} $text")
            case LogLevel.Error => logger.error(s"${colorMode.error(title)} $text")
          }
        })
        .void
    }

  private def console[F[_]: {Monad, Defer}](
    loggerName: LoggerName,
    translator: Translator[F, String],
    colorMode: ColorMode)(using C: Console[F]) = {
    val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    def stdout(ts: ZonedDateTime, level: LogLevel, message: String): F[Unit] =
      C.println(show"${fmt.format(ts)} $level [${loggerName.value}] $message")

    LogSink { (event: Event) =>
      translator
        .translate(event)
        .flatMap(_.traverse { text =>
          val ts = event.timestamp.value
          val title = eventTitle(event)
          eventLogLevel[F, Unit](event).run {
            case lvl @ LogLevel.Debug => stdout(ts, lvl, s"${colorMode.debug(title)} $text")
            case lvl @ LogLevel.Info  => stdout(ts, lvl, s"${colorMode.info(title)} $text")
            case lvl @ LogLevel.Good  => stdout(ts, lvl, s"${colorMode.good(title)} $text")
            case lvl @ LogLevel.Warn  => stdout(ts, lvl, s"${colorMode.warn(title)} $text")
            case lvl @ LogLevel.Error => stdout(ts, lvl, s"${colorMode.error(title)} $text")
          }
        })
        .void
    }
  }

  private def eventLogSink[F[_]: {Console, Sync}](logFormat: LogFormat, loggerName: LoggerName): LogSink[F] =
    logFormat match {
      /*
       * console
       */
      case LogFormat.Console_PlainText =>
        console[F](
          loggerName = loggerName,
          translator = SimpleTextTranslator[F],
          colorMode = ColorMode.render
        )
      case LogFormat.Console_Json =>
        console[F](
          loggerName = loggerName,
          translator = PrettyJsonTranslator[F].map(_.noSpaces),
          colorMode = ColorMode.render
        )
      case LogFormat.Console_Json_MultiLine =>
        console[F](
          loggerName = loggerName,
          translator = PrettyJsonTranslator[F].map(_.spaces2),
          colorMode = ColorMode.render
        )
      case LogFormat.Console_Json_Verbose =>
        console[F](
          loggerName = loggerName,
          translator = Translator.idTranslator[F].map(_.asJson.spaces2),
          colorMode = ColorMode.render
        )
      case LogFormat.Console_Json_NoColor =>
        console[F](
          loggerName = loggerName,
          translator = PrettyJsonTranslator[F].map(_.noSpaces),
          colorMode = ColorMode.none
        )
      /*
       * slf4j
       */
      case LogFormat.Slf4j_PlainText =>
        slf4j[F](
          logger = Slf4jLogger.getLoggerFromName[F](loggerName.value),
          translator = SimpleTextTranslator[F],
          colorMode = ColorMode.render
        )
      case LogFormat.Slf4j_Json =>
        slf4j[F](
          logger = Slf4jLogger.getLoggerFromName[F](loggerName.value),
          translator = PrettyJsonTranslator[F].map(_.noSpaces),
          colorMode = ColorMode.render
        )
      case LogFormat.Slf4j_Json_NoColor =>
        slf4j[F](
          logger = Slf4jLogger.getLoggerFromName[F](loggerName.value),
          translator = PrettyJsonTranslator[F].map(_.noSpaces),
          colorMode = ColorMode.none
        )
    }
end EventLogSink
