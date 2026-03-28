package com.github.chenharryhua.nanjin.guard.service.logging

import cats.effect.kernel.Sync
import cats.effect.std.Console
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.guard.config.LogFormat
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.translator.{
  eventTitle,
  ColorScheme,
  PrettyJsonTranslator,
  SimpleTextTranslator,
  Translator
}
import io.circe.syntax.EncoderOps
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{LoggerName, MessageLogger}

import java.time.ZoneId

final class LogSink[F[_]: Sync](
  logger: MessageLogger[F],
  translator: Translator[F, String],
  logColor: LogColor
) {
  def write(event: Event): F[Unit] =
    translator
      .translate(event)
      .flatMap(_.traverse { text =>
        ColorScheme.decorate[F, Unit](event).run {
          case ColorScheme.GoodColor  => logger.info(s"${logColor.good(eventTitle(event))} $text")
          case ColorScheme.InfoColor  => logger.info(s"${logColor.info(eventTitle(event))} $text")
          case ColorScheme.WarnColor  => logger.warn(s"${logColor.warn(eventTitle(event))} $text")
          case ColorScheme.ErrorColor => logger.error(s"${logColor.error(eventTitle(event))} $text")
          case ColorScheme.DebugColor => logger.debug(s"${logColor.debug(eventTitle(event))} $text")
        }
      })
      .void
}

object LogSink {
  def apply[F[_]: {Sync, Console}](logFormat: LogFormat, zoneId: ZoneId, loggerName: LoggerName): LogSink[F] =
    logFormat match {
      /*
       * console
       */
      case LogFormat.Console_PlainText =>
        new LogSink[F](
          translator = SimpleTextTranslator[F],
          logger = new ConsoleLogger[F](zoneId, loggerName),
          logColor = LogColor.console
        )
      case LogFormat.Console_Json_OneLine =>
        new LogSink[F](
          translator = PrettyJsonTranslator[F].map(_.noSpaces),
          logger = new ConsoleLogger[F](zoneId, loggerName),
          logColor = LogColor.console
        )
      case LogFormat.Console_Json_MultiLine =>
        new LogSink[F](
          translator = PrettyJsonTranslator[F].map(_.spaces2),
          logger = new ConsoleLogger[F](zoneId, loggerName),
          logColor = LogColor.console
        )
      case LogFormat.Console_JsonVerbose =>
        new LogSink[F](
          translator = Translator.idTranslator.map(_.asJson.spaces2),
          logger = new ConsoleLogger[F](zoneId, loggerName),
          logColor = LogColor.console
        )

      /*
       * slf4j
       */
      case LogFormat.Slf4j_PlainText =>
        new LogSink[F](
          translator = SimpleTextTranslator[F],
          logger = Slf4jLogger.getLoggerFromName[F](loggerName.value),
          logColor = LogColor.none
        )
      case LogFormat.Slf4j_Json_OneLine =>
        new LogSink[F](
          translator = PrettyJsonTranslator[F].map(_.noSpaces),
          logger = Slf4jLogger.getLoggerFromName[F](loggerName.value),
          logColor = LogColor.none
        )
      case LogFormat.Slf4j_Json_MultiLine =>
        new LogSink[F](
          translator = PrettyJsonTranslator[F].map(_.spaces2),
          logger = Slf4jLogger.getLoggerFromName[F](loggerName.value),
          logColor = LogColor.none
        )
    }
}
