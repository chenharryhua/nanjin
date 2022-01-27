package com.github.chenharryhua.nanjin.guard.observers

import cats.Monad
import cats.effect.std.Console
import cats.implicits.{toFunctorOps, toShow}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.translators.{Translator, UpdateTranslator}
import fs2.Chunk

import java.time.format.DateTimeFormatter

object console {
  def apply[F[_]: Console: Monad](translator: Translator[F, String]): TextConsole[F] = new TextConsole[F](translator)

  def apply[F[_]: Console: Monad]: TextConsole[F] = new TextConsole[F](Translator.text[F])
}

object simple {
  def apply[F[_]: Console: Monad]: TextConsole[F] = new TextConsole[F](Translator.simpleText[F])
}

final class TextConsole[F[_]: Monad](translator: Translator[F, String])(implicit C: Console[F])
    extends (NJEvent => F[Unit]) with UpdateTranslator[F, String, TextConsole[F]] {

  override def updateTranslator(f: Translator[F, String] => Translator[F, String]): TextConsole[F] =
    new TextConsole[F](f(translator))

  private[this] val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  override def apply(event: NJEvent): F[Unit] =
    translator
      .translate(event)
      .flatMap(_.traverse(evt => C.println(s"${event.zonedDateTime.format(fmt)} Console $evt")).void)
  def chunk(events: Chunk[NJEvent]): F[Unit] = events.traverse(apply).void
}
