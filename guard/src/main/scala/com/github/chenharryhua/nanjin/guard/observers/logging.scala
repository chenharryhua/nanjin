package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.Sync
import cats.implicits.{toFunctorOps, toTraverseOps}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{
  ActionFail,
  ActionRetry,
  InstantAlert,
  ServicePanic
}
import com.github.chenharryhua.nanjin.guard.translators.{Translator, UpdateTranslator}
import fs2.Chunk
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object logging {
  def apply[F[_]: Sync](translator: Translator[F, String]): TextLogging[F] = new TextLogging[F](translator)

  def verbose[F[_]: Sync]: TextLogging[F] = apply[F](Translator.verboseText[F])

  def simple[F[_]: Sync]: TextLogging[F] = apply(Translator.simpleText[F])

  final class TextLogging[F[_]: Sync](translator: Translator[F, String])
      extends (NJEvent => F[Unit]) with UpdateTranslator[F, String, TextLogging[F]] {

    private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

    override def updateTranslator(f: Translator[F, String] => Translator[F, String]): TextLogging[F] =
      new TextLogging[F](f(translator))

    override def apply(event: NJEvent): F[Unit] =
      event match {
        case sa: InstantAlert =>
          translator.instantAlert.run(sa).value.flatMap(_.traverse(logger.warn(_))).void
        case sp: ServicePanic =>
          translator.servicePanic.run(sp).value.flatMap(_.traverse(o => logger.error(o))).void
        case ar: ActionRetry =>
          translator.actionRetry.run(ar).value.flatMap(_.traverse(o => logger.warn(o))).void
        case af: ActionFail =>
          translator.actionFail.run(af).value.flatMap(_.traverse(o => logger.error(o))).void
        case others => translator.translate(others).flatMap(_.traverse(m => logger.info(m))).void
      }

    def chunk(events: Chunk[NJEvent]): F[Unit] = events.traverse(apply).void

  }
}
