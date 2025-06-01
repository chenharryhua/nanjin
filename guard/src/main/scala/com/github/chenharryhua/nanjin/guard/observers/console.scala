package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.std.Console
import cats.syntax.all.*
import cats.{Endo, Monad}
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.translator.{textHelper, ColorScheme, Translator, UpdateTranslator}
import io.circe.syntax.EncoderOps

import java.time.format.DateTimeFormatter

object console {
  def apply[F[_]: Console: Monad](translator: Translator[F, String]): TextConsole[F] =
    new TextConsole[F](translator)

  def text[F[_]: Console: Monad]: TextConsole[F] =
    apply[F](SimpleTextTranslator[F])

  def json[F[_]: Console: Monad]: TextConsole[F] =
    apply[F](PrettyJsonTranslator[F].map(_.noSpaces))

  def verbose[F[_]: Console: Monad]: TextConsole[F] =
    apply[F](Translator.idTranslator.map(_.asJson.spaces2))

  final class TextConsole[F[_]: Console: Monad](translator: Translator[F, String])
      extends (Event => F[Unit]) with UpdateTranslator[F, String, TextConsole[F]] {
    private[this] val C                            = Console[F]
    private[this] def coloring(evt: Event): String =
      ColorScheme.decorate(evt).run(textHelper.consoleColor).value

    override def updateTranslator(f: Endo[Translator[F, String]]): TextConsole[F] =
      new TextConsole[F](f(translator))

    private[this] val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    override def apply(event: Event): F[Unit] =
      translator
        .translate(event)
        .flatMap(_.traverse(evt =>
          C.println(s"${event.timestamp.format(fmt)} ${coloring(event)} - $evt")).void)
  }
}
