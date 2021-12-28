package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.Sync
import cats.implicits.{toFunctorOps, toShow, toTraverseOps}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.Chunk
import io.circe.Json
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object logging {
  def json[F[_]: Sync](f: Json => String): JsonLogging[F] = new JsonLogging[F](Translator.json[F], f)
  def text[F[_]: Sync]: TextLogging[F]                    = new TextLogging[F](Translator.text[F])
}

final class JsonLogging[F[_]: Sync] private[observers] (translator: Translator[F, Json], jsonConverter: Json => String)
    extends (NJEvent => F[Unit]) {
  def updateTranslator(f: Translator[F, Json] => Translator[F, Json]): JsonLogging[F] =
    new JsonLogging[F](f(translator), jsonConverter)

  private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  override def apply(event: NJEvent): F[Unit] =
    event match {
      case sp @ ServicePanic(_, _, _, error) =>
        translator.servicePanic
          .run(sp)
          .value
          .flatMap(oj => (oj, error.throwable).traverseN { case (j, ex) => logger.error(ex)(jsonConverter(j)) }.void)
      case sa: ServiceAlert =>
        translator.serviceAlert.run(sa).value.flatMap(_.traverse(j => logger.warn(jsonConverter(j))).void)
      case ar @ ActionRetrying(_, _, _, error) =>
        translator.actionRetrying
          .run(ar)
          .value
          .flatMap(oj => (oj, error.throwable).traverseN { case (j, ex) => logger.warn(ex)(jsonConverter(j)) }.void)
      case af @ ActionFailed(_, _, _, _, error) =>
        translator.actionFailed
          .run(af)
          .value
          .flatMap(oj => (oj, error.throwable).traverseN { case (j, ex) => logger.warn(ex)(jsonConverter(j)) }.void)
      case others => translator.translate(others).flatMap(_.traverse(m => logger.info(m.spaces2)).void)
    }

  def chunk(events: Chunk[NJEvent]): F[Unit] = events.traverse(apply).void
}

final class TextLogging[F[_]: Sync] private[observers] (translator: Translator[F, String])
    extends (NJEvent => F[Unit]) {
  def updateTranslator(f: Translator[F, String] => Translator[F, String]): TextLogging[F] =
    new TextLogging[F](f(translator))

  private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  override def apply(event: NJEvent): F[Unit] =
    event match {
      case sp @ ServicePanic(_, _, _, error) =>
        translator.servicePanic
          .run(sp)
          .value
          .flatMap(oj => (oj, error.throwable).traverseN { case (j, ex) => logger.error(ex)(j) }.void)
      case sa: ServiceAlert =>
        translator.serviceAlert.run(sa).value.flatMap(_.traverse(j => logger.warn(j)).void)
      case ar @ ActionRetrying(_, _, _, error) =>
        translator.actionRetrying
          .run(ar)
          .value
          .flatMap(oj => (oj, error.throwable).traverseN { case (j, ex) => logger.warn(ex)(j) }.void)
      case af @ ActionFailed(_, _, _, _, error) =>
        translator.actionFailed
          .run(af)
          .value
          .flatMap(oj => (oj, error.throwable).traverseN { case (j, ex) => logger.warn(ex)(j) }.void)
      case others => translator.translate(others).flatMap(_.traverse(m => logger.info(m)).void)
    }

  def chunk(events: Chunk[NJEvent]): F[Unit] = events.traverse(apply).void
}
