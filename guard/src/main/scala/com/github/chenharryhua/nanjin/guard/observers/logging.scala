package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.Sync
import cats.implicits.toFunctorOps
import cats.syntax.all.*
import cats.{Endo, Eval}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.translators.{ColorScheme, Translator, UpdateTranslator}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{LoggerName, SelfAwareStructuredLogger}

object logging {
  def apply[F[_]: Sync](translator: Translator[F, String]): TextLogging[F] =
    new TextLogging[F](translator, LoggerName("nanjin"))

  def simple[F[_]: Sync]: TextLogging[F] = apply(Translator.simpleText[F])
  def json[F[_]: Sync]: TextLogging[F]   = apply(Translator.prettyJson[F].map(_.noSpaces))

  final class TextLogging[F[_]: Sync](translator: Translator[F, String], loggerName: LoggerName)
      extends (NJEvent => F[Unit]) with UpdateTranslator[F, String, TextLogging[F]] {

    private lazy val logger: SelfAwareStructuredLogger[F] =
      Slf4jLogger.getLogger[F](Sync[F], loggerName)

    def withLoggerName(loggerName: String): TextLogging[F] =
      new TextLogging[F](translator, LoggerName(loggerName))

    override def updateTranslator(f: Endo[Translator[F, String]]): TextLogging[F] =
      new TextLogging[F](f(translator), loggerName)

    override def apply(event: NJEvent): F[Unit] =
      translator.translate(event).flatMap {
        case Some(message) =>
          ColorScheme.decorate(event).run(Eval.now).value match {
            case ColorScheme.GoodColor  => logger.info(message)
            case ColorScheme.InfoColor  => logger.info(message)
            case ColorScheme.WarnColor  => logger.warn(message)
            case ColorScheme.ErrorColor => logger.error(message)
          }
        case None => Sync[F].unit
      }
  }
}
