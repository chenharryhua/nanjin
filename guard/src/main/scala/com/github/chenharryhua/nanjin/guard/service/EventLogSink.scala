package com.github.chenharryhua.nanjin.guard.service

import cats.Monad
import cats.effect.kernel.Sync
import cats.effect.std.Console
import cats.implicits.showInterpolator
import cats.syntax.applicative.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.config.{LogFormat, Service, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.translator.{
  eventLogLevel,
  AnsiTextTranslator,
  PrettyJsonTranslator,
  Translator
}
import io.circe.syntax.EncoderOps
import org.slf4j.{Logger, LoggerFactory}

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

private object EventLogSink:
  def apply[F[_]: {Console, Sync}](serviceParams: ServiceParams): LogSink[F] =
    serviceParams.logFormat match {
      case Some(format) =>
        eventLogSink[F](logFormat = format, service = serviceParams.serviceIdentity.service)
      case None => LogSink(_ => ().pure[F])
    }

  private def slf4JLogSink[F[_]](logger: Logger, translator: Translator[F, String])(using
    F: Sync[F]): LogSink[F] =
    LogSink { (event: Event) =>
      translator
        .translate(event)
        .flatMap(_.traverse { text =>
          eventLogLevel[F, Unit](event).run {
            case LogLevel.Debug => F.blocking(logger.debug(text))
            case LogLevel.Info  => F.blocking(logger.info(text))
            case LogLevel.Good  => F.blocking(logger.info(text))
            case LogLevel.Warn  => F.blocking(logger.warn(text))
            case LogLevel.Error => F.blocking(logger.error(text))
          }
        }.void)
    }

  private def consoleLogSink[F[_]: {Monad, Console}](
    service: Service,
    translator: Translator[F, String]): LogSink[F] = {
    val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    LogSink { (event: Event) =>
      translator
        .translate(event)
        .flatMap(_.traverse { text =>
          Console[F].println(show"${fmt.format(event.timestamp.value)} [${service.value}] $text")
        })
        .void
    }
  }

  private def eventLogSink[F[_]: {Console, Sync}](logFormat: LogFormat, service: Service): LogSink[F] =
    logFormat match {
      case LogFormat.ConsolePlainText =>
        consoleLogSink[F](service, AnsiTextTranslator[F])
      case LogFormat.ConsoleJson =>
        consoleLogSink[F](service, PrettyJsonTranslator[F].map(_.noSpaces))
      case LogFormat.ConsoleJsonMultiLine =>
        consoleLogSink[F](service, PrettyJsonTranslator[F].map(_.spaces2))
      case LogFormat.ConsoleJsonVerbose =>
        consoleLogSink[F](service, Translator.idTranslator[F].map(_.asJson.spaces2))
      case LogFormat.Slf4jJson =>
        slf4JLogSink[F](LoggerFactory.getLogger(service.value), PrettyJsonTranslator[F].map(_.noSpaces))
    }
end EventLogSink
