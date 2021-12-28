package com.github.chenharryhua.nanjin.guard.observers

import cats.Monad
import cats.effect.std.Console
import cats.implicits.{toFunctorOps, toShow}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import fs2.Chunk
import io.circe.Json

object console {
  def json[F[_]: Console: Monad]: JsonConsole[F] =
    new JsonConsole[F](Translator.json[F])

  def text[F[_]: Console: Monad]: TextConsole[F] =
    new TextConsole[F](Translator.text[F])

}

final class JsonConsole[F[_]: Monad] private[observers] (translator: Translator[F, Json])(implicit C: Console[F])
    extends (NJEvent => F[Unit]) {

  def updateTranslator(f: Translator[F, Json] => Translator[F, Json]) = new JsonConsole[F](f(translator))

  override def apply(event: NJEvent): F[Unit] =
    translator.translate(event).flatMap(_.traverse(j => C.println(j.spaces2)).void)

  def chunk(events: Chunk[NJEvent]): F[Unit] = events.traverse(apply).void
}

final class TextConsole[F[_]: Monad] private[observers] (translator: Translator[F, String])(implicit C: Console[F])
    extends (NJEvent => F[Unit]) {

  def updateTranslator(f: Translator[F, String] => Translator[F, String]) = new TextConsole[F](f(translator))

  override def apply(event: NJEvent): F[Unit] = translator.translate(event).flatMap(_.traverse(C.println).void)
  def chunk(events: Chunk[NJEvent]): F[Unit]  = events.traverse(apply).void
}
