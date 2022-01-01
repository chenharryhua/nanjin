package com.github.chenharryhua.nanjin.guard.observers

import cats.Monad
import cats.effect.std.Console
import cats.implicits.{toFunctorOps, toShow}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.translators.{Translator, UpdateTranslator}
import fs2.Chunk

object console {
  def apply[F[_]: Console: Monad](translator: Translator[F, String]): TextConsole[F] = new TextConsole[F](translator)

  def apply[F[_]: Console: Monad]: TextConsole[F] = new TextConsole[F](Translator.text[F])
}

final class TextConsole[F[_]: Monad] private[observers] (translator: Translator[F, String])(implicit C: Console[F])
    extends (NJEvent => F[Unit]) with UpdateTranslator[F, String, TextConsole[F]] {

  override def updateTranslator(f: Translator[F, String] => Translator[F, String]): TextConsole[F] =
    new TextConsole[F](f(translator))

  override def apply(event: NJEvent): F[Unit] = translator.translate(event).flatMap(_.traverse(C.println).void)
  def chunk(events: Chunk[NJEvent]): F[Unit]  = events.traverse(apply).void
}
