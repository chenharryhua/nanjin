package com.github.chenharryhua.nanjin.terminals

import cats.syntax.show.showInterpolator
import com.github.chenharryhua.nanjin.common.DurationFormatter
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, HCursor, Json}
import io.lemonlabs.uri.Url
import org.typelevel.cats.time.zoneddatetimeInstances

import java.time.{Duration, Instant, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.UUID

/** Instruction to create a new rotated file.
  *
  * This event marks the beginning of a new `Tick` window. A writer opened from this information will accept
  * records that belong to that tick.
  *
  * @param sequenceId
  *   globally unique id for the entire rotation stream
  * @param index
  *   1-based, strictly increasing number of the file within the sequence
  * @param time
  *   timestamp when the sink starts writing records for this file
  */
final case class CreateRotateFile(
  sequenceId: UUID,
  index: Long,
  time: ZonedDateTime
)

/** Result of a completed rotation window.
  *
  * One `RotateFile` is emitted for every finished `Tick`.
  *
  * The file identified by `url` contains exactly `recordCount` records written while that tick window was
  * active.
  *
  * Invariants typically guaranteed by rotation sinks:
  *
  *   - the sum of `recordCount` equals the total number of input records
  *   - records in this file belong exclusively to the time window
  *   - open: the moment when the file open for write
  *   - close: the moment when the file is closed
  */

final case class RotateFile(create: CreateRotateFile, closed: Instant, url: Url, recordCount: Long) {
  val window: Duration = Duration.between(create.time.toInstant, closed)
}

object RotateFile {
  given Encoder[RotateFile] =
    (a: RotateFile) =>
      Json.obj(
        "index" -> Json.fromLong(a.create.index),
        "url" -> a.url.asJson,
        "recordCount" -> Json.fromLong(a.recordCount),
        "create" -> a.create.time.toLocalDateTime.asJson,
        "closed" -> a.closed.atZone(a.create.time.getZone).toLocalDateTime.asJson,
        "window" -> DurationFormatter.defaultFormatter.format(a.window).asJson,
        "zoneId" -> a.create.time.getZone.asJson,
        "sequenceId" -> a.create.sequenceId.asJson
      )

  given Decoder[RotateFile] =
    (c: HCursor) =>
      for {
        sid <- c.get[UUID]("sequenceId")
        idx <- c.get[Long]("index")
        zoneId <- c.get[ZoneId]("zoneId")
        create <- c.get[LocalDateTime]("create")
        closed <- c.get[LocalDateTime]("closed")
        url <- c.get[Url]("url")
        recordCount <- c.get[Long]("recordCount")
      } yield RotateFile(
        create = CreateRotateFile(
          sequenceId = sid,
          index = idx,
          time = create.atZone(zoneId)
        ),
        closed = closed.atZone(zoneId).toInstant,
        url = url,
        recordCount = recordCount
      )
}

final case class RotateWriteException(create: CreateRotateFile, url: Url, offset: Long, throwable: Throwable)
    extends Exception(
      show"fail writing $url offset=$offset, index=${create.index}, create=${create.time}",
      throwable)
