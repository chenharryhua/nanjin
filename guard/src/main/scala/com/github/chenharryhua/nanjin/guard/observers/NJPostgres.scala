package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Resource, Sync}
import com.github.chenharryhua.nanjin.common.database.TableName
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.translators.{Translator, UpdateTranslator}
import fs2.{Pipe, Stream}
import io.circe.Json
import skunk.circe.codec.json.json
import skunk.data.Completion.Insert
import skunk.implicits.toStringOps
import skunk.{Command, Session}

object postgres {
  def apply[F[_]: Sync](session: Resource[F, Session[F]]): NJPostgres[F] =
    new NJPostgres[F](session, Translator.json[F], tableName = TableName("event_stream"))
}

final class NJPostgres[F[_]: Sync](
  session: Resource[F, Session[F]],
  translator: Translator[F, Json],
  tableName: TableName)
    extends Pipe[F, NJEvent, Int] with UpdateTranslator[F, Json, NJPostgres[F]] {

  def withTableName(tableName: TableName): NJPostgres[F] =
    new NJPostgres[F](session, translator, tableName)

  override def updateTranslator(f: Translator[F, Json] => Translator[F, Json]): NJPostgres[F] =
    new NJPostgres[F](session, f(translator), tableName)

  def apply(events: Stream[F, NJEvent]): Stream[F, Int] = {
    val cmd: Command[Json] = sql"INSERT INTO #${tableName.value} VALUES ($json)".command
    for {
      ss <- Stream.resource(session)
      complete <- events.evalMap(translator.translate).unNone.through(ss.pipe(cmd))
    } yield complete match {
      case Insert(count) => count
      case _             => 0
    }
  }
}
