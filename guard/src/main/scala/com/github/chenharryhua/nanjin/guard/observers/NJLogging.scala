package com.github.chenharryhua.nanjin.guard.observers

import cats.data.Reader
import cats.effect.kernel.Sync
import cats.implicits.{toFunctorOps, toShow}
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.Chunk
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object logging {
  def apply[F[_]: Sync](f: NJEvent => String): NJLogging[F] = new NJLogging[F](Reader(f))
}

final class NJLogging[F[_]] private[observers] (converter: Reader[NJEvent, String])(implicit F: Sync[F])
    extends (NJEvent => F[Unit]) {
  private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  override def apply(event: NJEvent): F[Unit] = {
    val out: String = converter.run(event)
    event match {
      case ServicePanic(_, _, _, _, error)    => error.throwable.fold(logger.error(out))(ex => logger.error(ex)(out))
      case _: ServiceAlert                    => logger.warn(out)
      case ActionRetrying(_, _, _, _, error)  => error.throwable.fold(logger.warn(out))(ex => logger.warn(ex)(out))
      case ActionFailed(_, _, _, _, _, error) => error.throwable.fold(logger.error(out))(ex => logger.error(ex)(out))
      case _                                  => logger.info(out)
    }
  }

  def chunk(events: Chunk[NJEvent]): F[Unit] = events.traverse(apply).void
}
