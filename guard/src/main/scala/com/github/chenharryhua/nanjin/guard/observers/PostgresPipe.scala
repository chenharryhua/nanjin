package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Resource, Temporal}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.database.TableName
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, ServiceStart}
import com.github.chenharryhua.nanjin.guard.translators.{Translator, UpdateTranslator}
import fs2.{Pipe, Stream}
import io.circe.Json
import skunk.circe.codec.json.json
import skunk.implicits.toStringOps
import skunk.{Command, Session}

import java.util.UUID

/** DDL:
  *
  * CREATE TABLE public.event_stream ( info json NULL, "timestamp" timestamptz NULL DEFAULT CURRENT_TIMESTAMP );
  */

object PostgresPipe {

  def apply[F[_]: Temporal](session: Resource[F, Session[F]]): PostgresPipe[F] =
    new PostgresPipe[F](session, Translator.simpleJson[F], TableName("event_stream"))

}

final class PostgresPipe[F[_]](session: Resource[F, Session[F]], translator: Translator[F, Json], tableName: TableName)(
  implicit F: Temporal[F])
    extends Pipe[F, NJEvent, NJEvent] with UpdateTranslator[F, Json, PostgresPipe[F]] {

  def withTableName(tableName: TableName): PostgresPipe[F] =
    new PostgresPipe[F](session, translator, tableName)

  override def updateTranslator(f: Translator[F, Json] => Translator[F, Json]): PostgresPipe[F] =
    new PostgresPipe[F](session, f(translator), tableName)

  def apply(events: Stream[F, NJEvent]): Stream[F, NJEvent] = {
    val cmd: Command[Json] = sql"INSERT INTO #${tableName.value} VALUES ($json)".command
    for {
      pg <- Stream.resource(session.flatMap(_.prepare(cmd)))
      ofm <- Stream.eval(F.ref[Map[UUID, ServiceStart]](Map.empty).map(r => new ObserverFinalizeMonitor(translator, r)))
      event <- events
        .evalTap(ofm.monitoring)
        .evalTap(evt => translator.translate(evt).flatMap(_.traverse(msg => pg.execute(msg).attempt)).void)
        .onFinalizeCase(ofm.terminated(_).flatMap(_.traverse(msg => pg.execute(msg).attempt)).void)
    } yield event
  }
}
