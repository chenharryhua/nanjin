package com.github.chenharryhua.nanjin.guard.logging

import cats.effect.kernel.Sync
import cats.effect.std.Console
import cats.syntax.applicative.catsSyntaxApplicativeId
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.traverse.toTraverseOps
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

sealed trait LogSink[F[_]]:
  def write(event: Event): F[Unit]
end LogSink


object LogSink {
  def apply[F[_]: {Sync, Console}](
    logFormat: LogFormat,
    zoneId: ZoneId,
    loggerName: LoggerName): F[LogSink[F]] =
    logFormat match {
      /*
       * console
       */
      case LogFormat.Console_PlainText =>
        new LogSinkImpl[F](
          translator = SimpleTextTranslator[F],
          logger = new ConsoleLogger[F](zoneId, loggerName),
          logColor = LogColor.console
        ).pure[F].widen
      case LogFormat.Console_Json_OneLine =>
        new LogSinkImpl[F](
          translator = PrettyJsonTranslator[F].map(_.noSpaces),
          logger = new ConsoleLogger[F](zoneId, loggerName),
          logColor = LogColor.console
        ).pure[F].widen
      case LogFormat.Console_Json_MultiLine =>
        new LogSinkImpl[F](
          translator = PrettyJsonTranslator[F].map(_.spaces2),
          logger = new ConsoleLogger[F](zoneId, loggerName),
          logColor = LogColor.console
        ).pure[F].widen
      case LogFormat.Console_JsonVerbose =>
        new LogSinkImpl[F](
          translator = Translator.idTranslator.map(_.asJson.spaces2),
          logger = new ConsoleLogger[F](zoneId, loggerName),
          logColor = LogColor.console
        ).pure[F].widen

      /*
       * slf4j
       */
      case LogFormat.Slf4j_PlainText =>
        Slf4jLogger
          .fromName[F](loggerName.value)
          .map(logger =>
            new LogSinkImpl[F](
              translator = SimpleTextTranslator[F],
              logger = logger,
              logColor = LogColor.none
            ))
      case LogFormat.Slf4j_Json_OneLine =>
        Slf4jLogger
          .fromName[F](loggerName.value)
          .map(logger =>
            new LogSinkImpl[F](
              translator = PrettyJsonTranslator[F].map(_.noSpaces),
              logger = logger,
              logColor = LogColor.none
            ))
      case LogFormat.Slf4j_Json_MultiLine =>
        Slf4jLogger
          .fromName[F](loggerName.value)
          .map(logger =>
            new LogSinkImpl[F](
              translator = PrettyJsonTranslator[F].map(_.spaces2),
              logger = logger,
              logColor = LogColor.none
            ))
    }

  final private class LogSinkImpl[F[_]: Sync](
    logger: MessageLogger[F],
    translator: Translator[F, String],
    logColor: LogColor
  ) extends LogSink[F] {

    override def write(event: Event): F[Unit] =
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
}
