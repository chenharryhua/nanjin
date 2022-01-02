package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.Sync
import cats.implicits.{toFunctorOps, toShow, toTraverseOps}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.translators.{Translator, UpdateTranslator}
import fs2.Chunk
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object logging {
  def apply[F[_]: Sync](translator: Translator[F, String]): TextLogging[F] = new TextLogging[F](translator)

  def apply[F[_]: Sync]: TextLogging[F] = new TextLogging[F](Translator.text[F])
}

final class TextLogging[F[_]: Sync] private[observers] (translator: Translator[F, String])
    extends (NJEvent => F[Unit]) with UpdateTranslator[F, String, TextLogging[F]] {

  private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  override def updateTranslator(f: Translator[F, String] => Translator[F, String]): TextLogging[F] =
    new TextLogging[F](f(translator))

  override def apply(event: NJEvent): F[Unit] =
    event match {
      case sa: ServiceAlert => translator.serviceAlert.run(sa).value.flatMap(_.traverse(logger.warn(_)).void)
      case sp @ ServicePanic(_, _, _, _, error) =>
        translator.servicePanic
          .run(sp)
          .value
          .flatMap(oj => (oj, error.throwable).traverseN { case (j, ex) => logger.error(ex)(j) }.void)
      case ar @ ActionRetry(_, _, _, error) =>
        translator.actionRetry
          .run(ar)
          .value
          .flatMap(oj => (oj, error.throwable).traverseN { case (j, ex) => logger.warn(ex)(j) }.void)
      case af @ ActionFail(_, _, _, _, error) =>
        translator.actionFail
          .run(af)
          .value
          .flatMap(oj => (oj, error.throwable).traverseN { case (j, ex) => logger.error(ex)(j) }.void)
      case others => translator.translate(others).flatMap(_.traverse(m => logger.info(m)).void)
    }

  def chunk(events: Chunk[NJEvent]): F[Unit] = events.traverse(apply).void

}
