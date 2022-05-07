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

object postgres {

  def apply[F[_]: Temporal](session: Resource[F, Session[F]], tableName: TableName): NJPostgresPipe[F] =
    new NJPostgresPipe[F](session, Translator.simpleJson[F], tableName)

  def apply[F[_]: Temporal](session: Resource[F, Session[F]]): NJPostgresPipe[F] =
    apply[F](session, TableName("event_stream"))
}

final class NJPostgresPipe[F[_]](
  session: Resource[F, Session[F]],
  translator: Translator[F, Json],
  tableName: TableName)(implicit F: Temporal[F])
    extends Pipe[F, NJEvent, NJEvent] with UpdateTranslator[F, Json, NJPostgresPipe[F]] {

  def withTableName(tableName: TableName): NJPostgresPipe[F] =
    new NJPostgresPipe[F](session, translator, tableName)

  override def updateTranslator(f: Translator[F, Json] => Translator[F, Json]): NJPostgresPipe[F] =
    new NJPostgresPipe[F](session, f(translator), tableName)

  def apply(events: Stream[F, NJEvent]): Stream[F, NJEvent] = {
    val cmd: Command[Json] = sql"INSERT INTO #${tableName.value} VALUES ($json)".command
    for {
      pg <- Stream.resource(session.flatMap(_.prepare(cmd)))
      ref <- Stream.eval(F.ref[Map[UUID, ServiceStart]](Map.empty))
      event <- events
        .evalTap(evt => updateRef(ref, evt))
        .evalTap(evt => translator.translate(evt).flatMap(_.traverse(msg => pg.execute(msg).attempt)).void)
        .onFinalize(serviceTerminateEvents(ref, translator).flatMap(_.traverse(msg => pg.execute(msg).attempt)).void)
    } yield event
  }
}