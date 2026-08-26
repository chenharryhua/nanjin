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
import com.github.chenharryhua.nanjin.guard.translator.{
  eventLogLevel,
  AnsiTextTranslator,
  PrettyJsonTranslator,
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
        eventLogSink[F](
          logFormat = format,
          loggerName = LoggerName(serviceParams.serviceIdentity.service.value))
      case None => LogSink(_ => ().pure[F])
    }

  private def slf4JLogSink[F[_]: {Monad, Defer}](
    logger: MessageLogger[F],
    translator: Translator[F, String]): LogSink[F] =
    LogSink { (event: Event) =>
      translator
        .translate(event)
        .flatMap(_.traverse { text =>
          eventLogLevel[F, Unit](event).run {
            case LogLevel.Debug => logger.debug(text)
            case LogLevel.Info  => logger.info(text)
            case LogLevel.Good  => logger.info(text)
            case LogLevel.Warn  => logger.warn(text)
            case LogLevel.Error => logger.error(text)
          }
        })
        .void
    }

  private def consoleLogSink[F[_]: {Monad, Console}](
    loggerName: LoggerName,
    translator: Translator[F, String]): LogSink[F] = {
    val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    LogSink { (event: Event) =>
      translator
        .translate(event)
        .flatMap(_.traverse { text =>
          Console[F].println(show"${fmt.format(event.timestamp.value)} [${loggerName.value}] $text")
        })
        .void
    }
  }

  private def eventLogSink[F[_]: {Console, Sync}](logFormat: LogFormat, loggerName: LoggerName): LogSink[F] =
    logFormat match {
      case LogFormat.Console_PlainText =>
        consoleLogSink[F](loggerName, AnsiTextTranslator[F])
      case LogFormat.Console_Json =>
        consoleLogSink[F](loggerName, PrettyJsonTranslator[F].map(_.noSpaces))
      case LogFormat.Console_Json_MultiLine =>
        consoleLogSink[F](loggerName, PrettyJsonTranslator[F].map(_.spaces2))
      case LogFormat.Console_Json_Verbose =>
        consoleLogSink[F](loggerName, Translator.idTranslator[F].map(_.asJson.spaces2))
      case LogFormat.Slf4j_Json =>
        slf4JLogSink[F](
          Slf4jLogger.getLoggerFromName[F](loggerName.value),
          PrettyJsonTranslator[F].map(_.noSpaces))
    }
end EventLogSink
