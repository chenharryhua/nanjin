package com.github.chenharryhua.nanjin.guard.observers.logging

import cats.{Endo, Eval}
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.observers.{PrettyJsonTranslator, SimpleTextTranslator}
import com.github.chenharryhua.nanjin.guard.translator.{ColorScheme, Translator, UpdateTranslator}
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.syntax.all.*

object log {
  def apply[F[_]: Sync](translator: Translator[F, String]): TextLogging[F] =
    new TextLogging[F](translator)

  def text[F[_]: Sync]: TextLogging[F] = apply(SimpleTextTranslator[F])
  def json[F[_]: Sync]: TextLogging[F] = apply(PrettyJsonTranslator[F].map(_.noSpaces))

  final class TextLogging[F[_]: Sync](translator: Translator[F, String])
      extends (NJEvent => F[Unit]) with UpdateTranslator[F, String, TextLogging[F]] {

    private[this] lazy val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

    override def updateTranslator(f: Endo[Translator[F, String]]): TextLogging[F] =
      new TextLogging[F](f(translator))

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
