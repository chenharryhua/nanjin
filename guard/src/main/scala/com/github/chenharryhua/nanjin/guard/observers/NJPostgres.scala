package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.database.TableName
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.translators.{Translator, UpdateTranslator}
import fs2.{Pipe, Stream}
import io.circe.Json
import skunk.Session
import skunk.circe.codec.json.json
import skunk.data.Completion
import skunk.implicits.toStringOps

object postgres {
  def apply[F[_]: Sync](session: Resource[F, Session[F]]): NJPostgres[F] =
    new NJPostgres[F](session, Translator.json[F], TableName("event_stream"))
}

final class NJPostgres[F[_]: Sync](
  session: Resource[F, Session[F]],
  translator: Translator[F, Json],
  tableName: TableName)
    extends Pipe[F, NJEvent, Option[Completion]] with UpdateTranslator[F, Json, NJPostgres[F]] {

  def withTableName(tableName: TableName): NJPostgres[F] = new NJPostgres[F](session, translator, tableName)

  override def updateTranslator(f: Translator[F, Json] => Translator[F, Json]): NJPostgres[F] =
    new NJPostgres[F](session, f(translator), tableName)

  def apply(events: Stream[F, NJEvent]): Stream[F, Option[Completion]] = {
    val cmd = sql"INSERT INTO #${tableName.value} VALUES ($json)".command
    for {
      ss <- Stream.resource(session)
      evt <- events
      r <- Stream.eval(ss.prepare(cmd).use(pc => translator.translate(evt).flatMap(_.traverse(e => pc.execute(e)))))
    } yield r
  }
}
