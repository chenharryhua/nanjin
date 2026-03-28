package com.github.chenharryhua.nanjin.guard.observers.postgres

import cats.Endo
import cats.effect.kernel.{Async, Resource}
import cats.syntax.applicativeError.given
import cats.syntax.apply.given
import cats.syntax.flatMap.given
import cats.syntax.foldable.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.database.TableName
import com.github.chenharryhua.nanjin.guard.config.ServiceId
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceStart
import com.github.chenharryhua.nanjin.guard.observers.FinalizeMonitor
import com.github.chenharryhua.nanjin.guard.translator.{PrettyJsonTranslator, Translator, UpdateTranslator}
import fs2.{Pipe, Stream}
import io.circe.Json
import org.typelevel.log4cats.slf4j.Slf4jLogger
import skunk.circe.codec.json.json
import skunk.syntax.stringcontext.sql
import skunk.{Command, PreparedCommand, Session}

/** DDL:
  *
  * CREATE TABLE public.event_stream ( info json NULL, "timestamp" timestamptz NULL DEFAULT CURRENT_TIMESTAMP
  * );
  */

object PostgresObserver {
  def apply[F[_]: Async](session: Resource[F, Session[F]]): PostgresObserver[F] =
    new PostgresObserver[F](session, PrettyJsonTranslator[F])
}

final class PostgresObserver[F[_]](session: Resource[F, Session[F]], translator: Translator[F, Json])(using
  F: Async[F])
    extends UpdateTranslator[F, Json, PostgresObserver[F]] {

  private val name: String = "Postgres Observer"

  override def updateTranslator(f: Endo[Translator[F, Json]]): PostgresObserver[F] =
    new PostgresObserver[F](session, f(translator))

  private def execute(pg: PreparedCommand[F, Json], msg: Json): F[Unit] =
    pg.execute(msg).void

  def observe(tableName: TableName): Pipe[F, Event, Event] = (events: Stream[F, Event]) => {
    val cmd: Command[Json] = sql"INSERT INTO #${tableName.value} VALUES ($json)".command
    for {
      pg <- Stream.resource(session.evalMap(_.prepare(cmd)))
      log <- Stream.eval(Slf4jLogger.create[F])
      _ <- Stream.eval(log.info(s"initialize $name"))
      ofm <- Stream.eval(
        F.ref[Map[ServiceId, ServiceStart]](Map.empty).map(new FinalizeMonitor(translator.translate, _)))
      event <- events
        .evalTap(ofm.monitoring)
        .evalTap { evt =>
          translator
            .translate(evt)
            .flatMap(_.traverse_(execute(pg, _)))
            .recoverWith(ex => log.error(ex)(name))
        }
        .onFinalize {
          ofm.terminated.flatMap(_.traverse_(execute(pg, _))) *> log.info(s"$name was closed")
        }
    } yield event
  }
}
