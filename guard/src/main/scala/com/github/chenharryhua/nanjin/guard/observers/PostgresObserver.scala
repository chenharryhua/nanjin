package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Clock, Concurrent, Resource}
import cats.syntax.all.*
import cats.Endo
import com.github.chenharryhua.nanjin.common.database.TableName
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ServiceStart
import com.github.chenharryhua.nanjin.guard.translators.{Translator, UpdateTranslator}
import fs2.{Pipe, Stream}
import io.circe.Json
import skunk.{Command, PreparedCommand, Session}
import skunk.circe.codec.json.json
import skunk.data.Completion
import skunk.implicits.toStringOps

import java.util.UUID

/** DDL:
  *
  * CREATE TABLE public.event_stream ( info json NULL, "timestamp" timestamptz NULL DEFAULT CURRENT_TIMESTAMP
  * );
  */

object PostgresObserver {
  def apply[F[_]: Concurrent: Clock](session: Resource[F, Session[F]]): PostgresObserver[F] =
    new PostgresObserver[F](session, Translator.simpleJson[F])
}

final class PostgresObserver[F[_]: Clock](session: Resource[F, Session[F]], translator: Translator[F, Json])(
  implicit F: Concurrent[F])
    extends UpdateTranslator[F, Json, PostgresObserver[F]] {

  override def updateTranslator(f: Endo[Translator[F, Json]]): PostgresObserver[F] =
    new PostgresObserver[F](session, f(translator))

  private def execute(pg: PreparedCommand[F, Json], msg: Json): F[Either[Throwable, Completion]] =
    pg.execute(msg).attempt

  def observe(tableName: TableName): Pipe[F, NJEvent, NJEvent] = (events: Stream[F, NJEvent]) => {
    val cmd: Command[Json] = sql"INSERT INTO #${tableName.value} VALUES ($json)".command
    for {
      pg <- Stream.resource(session.flatMap(_.prepare(cmd)))
      ofm <- Stream.eval(
        F.ref[Map[UUID, ServiceStart]](Map.empty).map(new FinalizeMonitor(translator.translate, _)))
      event <- events
        .evalTap(ofm.monitoring)
        .evalTap(evt => translator.translate(evt).flatMap(_.traverse(execute(pg, _))).void)
        .onFinalizeCase(ofm.terminated(_).flatMap(_.traverse(execute(pg, _))).void)
    } yield event
  }
}
