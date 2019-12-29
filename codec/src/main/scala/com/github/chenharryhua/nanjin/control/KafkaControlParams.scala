package com.github.chenharryhua.nanjin.control

import java.time._

import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.string.Uri
import monocle.macros.Lenses

import scala.concurrent.duration.{FiniteDuration, _}

final case class StorageRootPath(uri: String Refined Uri) {
  val root: String = if (uri.value.endsWith("/")) uri.value else uri.value + "/"

  def +(sub: String): String = root + sub
}

object StorageRootPath {
  val default: StorageRootPath = StorageRootPath("./data/")
}

@Lenses final case class UploadRate(batchSize: Int, duration: FiniteDuration)

object UploadRate {
  val default: UploadRate = UploadRate(1000, 1.second)
}

@Lenses final case class KafkaControlParams private (
  timeRange: NJDateTimeRange,
  zoneId: ZoneId,
  rootPath: StorageRootPath) {

  val clock: Clock = Clock.system(zoneId)

  def withZoneId(zoneId: ZoneId): KafkaControlParams = copy(zoneId = zoneId)

  private def setStartTime(ts: NJTimestamp): KafkaControlParams =
    KafkaControlParams.timeRange.composeLens(NJDateTimeRange.start).set(Some(ts))(this)

  private def setEndTime(ts: NJTimestamp): KafkaControlParams =
    KafkaControlParams.timeRange.composeLens(NJDateTimeRange.end).set(Some(ts))(this)

  def withStartTime(dt: Instant): KafkaControlParams       = setStartTime(NJTimestamp(dt))
  def withEndTime(dt: Instant): KafkaControlParams         = setEndTime(NJTimestamp(dt))
  def withStartTime(dt: ZonedDateTime): KafkaControlParams = setStartTime(NJTimestamp(dt))
  def withEndTime(dt: ZonedDateTime): KafkaControlParams   = setEndTime(NJTimestamp(dt))
  def withStartTime(dt: LocalDateTime): KafkaControlParams = setStartTime(NJTimestamp(dt, zoneId))
  def withEndTime(dt: LocalDateTime): KafkaControlParams   = setEndTime(NJTimestamp(dt, zoneId))
  def withStartTime(dt: LocalDate): KafkaControlParams     = setStartTime(NJTimestamp(dt, zoneId))
  def withEndTime(dt: LocalDate): KafkaControlParams       = setEndTime(NJTimestamp(dt, zoneId))

  def withinOneDay(dt: LocalDate): KafkaControlParams =
    withStartTime(dt).withEndTime(dt.plusDays(1))

  def withToday: KafkaControlParams     = withinOneDay(LocalDate.now)
  def withYesterday: KafkaControlParams = withinOneDay(LocalDate.now.minusDays(1))

}

object SparKafkaParams {

  val default: KafkaControlParams = KafkaControlParams(
    NJDateTimeRange.infinite,
    ZoneId.systemDefault(),
    StorageRootPath.default
  )
}
