package com.github.chenharryhua.nanjin.terminals

import cats.syntax.show.showInterpolator
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, HCursor, Json}
import io.lemonlabs.uri.Url
import org.typelevel.cats.time.zoneddatetimeInstances

import java.time.{Duration, Instant, LocalDateTime, OffsetDateTime, ZoneId, ZonedDateTime}
import java.util.UUID

/** Instruction to create a new rotated file.
  *
  * The verb-phrase name is intentional: this is a command/event value (an instruction to open a file), the
  * counterpart to the past-tense `RotateFile` (the record of a file that was completed). It is not a function
  * despite reading like one.
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
  * What the rotation sinks guarantee:
  *
  *   - the sum of `recordCount` over the emitted files equals the number of input elements the sink consumed,
  *     which is not the same as the number of elements upstream offered: the sinks run the data against the
  *     tick stream with `mergeHaltBoth`, so under a finite policy the pipe halts when the ticks run out and
  *     the remaining input is never written
  *   - records in this file belong exclusively to the time window
  *   - `create.time`: the moment when the file was opened for writing
  *   - `closed`: the moment when the file was closed
  */

final case class RotateFile(create: CreateRotateFile, closed: Instant, url: Url, recordCount: Long) {
  val window: Duration = Duration.between(create.time.toInstant, closed)
}

object RotateFile {

  /** `create` and `closed` carry an OFFSET, e.g. `"2024-04-07T02:30:00+10:00"`, and that is load-bearing: a
    * local date-time plus a zone does not determine an instant. A zone is a schedule of offsets, so during a
    * fall-back overlap the same local reading has two instants, and `LocalDateTime.atZone` always picks the
    * earlier one - which decoded any file opened in the second pass an hour early and could shrink, zero or
    * invert `window`. The encoder already knew the offset and used to discard it with `.toLocalDateTime`.
    *
    * `zoneId` is still written and still needed: the offset alone cannot rebuild the zone the sink ran in.
    * `OffsetDateTime` is preferred over `ZonedDateTime` because circe renders the latter as
    * `2024-04-07T02:30+10:00[Australia/Sydney]`, duplicating `zoneId`. This mirrors `Tick`'s wire format.
    */
  given Encoder[RotateFile] =
    (a: RotateFile) =>
      Json.obj(
        "index" -> Json.fromLong(a.create.index),
        "url" -> a.url.asJson,
        "recordCount" -> Json.fromLong(a.recordCount),
        "create" -> a.create.time.toOffsetDateTime.asJson,
        "closed" -> a.closed.atZone(a.create.time.getZone).toOffsetDateTime.asJson,
        "window" -> defaultFormatter.format(a.window).asJson,
        "zoneId" -> a.create.time.getZone.asJson,
        "sequenceId" -> a.create.sequenceId.asJson
      )

  /** Read a timestamp in either form: the offset-bearing one written now, or a legacy bare local date-time,
    * which `zoneId` interprets with exactly the ambiguity it always had. The two circe decoders are mutually
    * exclusive - a bare string fails `ISO_OFFSET_DATE_TIME` and an offset-bearing string fails
    * `ISO_LOCAL_DATE_TIME` - so the fallback cannot mask a real failure. `zoneId` must therefore be read
    * before the timestamps.
    */
  private def read_zoned(c: HCursor, key: String, zoneId: ZoneId): Decoder.Result[ZonedDateTime] =
    c.get[OffsetDateTime](key)
      .map(_.atZoneSameInstant(zoneId))
      .orElse(c.get[LocalDateTime](key).map(_.atZone(zoneId)))

  given Decoder[RotateFile] =
    (c: HCursor) =>
      for {
        sid <- c.get[UUID]("sequenceId")
        idx <- c.get[Long]("index")
        zoneId <- c.get[ZoneId]("zoneId")
        create <- read_zoned(c, "create", zoneId)
        closed <- read_zoned(c, "closed", zoneId)
        url <- c.get[Url]("url")
        recordCount <- c.get[Long]("recordCount")
      } yield RotateFile(
        create = CreateRotateFile(
          sequenceId = sid,
          index = idx,
          time = create
        ),
        closed = closed.toInstant,
        url = url,
        recordCount = recordCount
      )
}

/** Raised when a write into the current rotated file fails.
  *
  * @param offset
  *   position in the target file at failure time: the number of records already written into this file before
  *   the failing write. Not a grand total (cf. `RotateFile.recordCount`, the completed file's count).
  */
final case class RotateWriteException(create: CreateRotateFile, url: Url, offset: Long, cause: Throwable)
    extends Exception(
      show"fail writing $url offset=$offset, index=${create.index}, create=${create.time}",
      cause)
