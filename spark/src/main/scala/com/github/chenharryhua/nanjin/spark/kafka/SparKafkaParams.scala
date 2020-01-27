package com.github.chenharryhua.nanjin.spark.kafka

import java.time._

import cats.data.Reader
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.common.TopicName
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka010.{LocationStrategies, LocationStrategy}

import scala.concurrent.duration._

@Lenses final case class NJRate(batchSize: Int, duration: FiniteDuration)

object NJRate {
  val default: NJRate = NJRate(batchSize = 1000, duration = 1.second)
}

@Lenses final case class ConversionTactics(keepPartition: Boolean, keepTimestamp: Boolean) {
  def withoutPartition: ConversionTactics = ConversionTactics.keepPartition.set(false)(this)
  def withoutTimestamp: ConversionTactics = ConversionTactics.keepTimestamp.set(false)(this)
  def withPartition: ConversionTactics    = ConversionTactics.keepPartition.set(true)(this)
  def withTimestamp: ConversionTactics    = ConversionTactics.keepTimestamp.set(true)(this)
}

object ConversionTactics {

  def default: ConversionTactics =
    ConversionTactics(keepPartition = false, keepTimestamp = true)
}

final case class KafkaPathBuild(
  timeRange: NJDateTimeRange,
  fileFormat: NJFileFormat,
  topicName: TopicName)

@Lenses final case class SparKafkaParams private (
  timeRange: NJDateTimeRange,
  conversionTactics: ConversionTactics,
  uploadRate: NJRate,
  pathBuilder: Reader[KafkaPathBuild, String],
  fileFormat: NJFileFormat,
  saveMode: SaveMode,
  locationStrategy: LocationStrategy,
  repartition: Int) {

  def getPath(topicName: TopicName): String =
    pathBuilder(KafkaPathBuild(timeRange, fileFormat, topicName))

  def withTimeRange(f: NJDateTimeRange => NJDateTimeRange): SparKafkaParams =
    SparKafkaParams.timeRange.modify(f)(this)

  def withConversionTactics(f: ConversionTactics => ConversionTactics): SparKafkaParams =
    SparKafkaParams.conversionTactics.modify(f)(this)

  val clock: Clock = Clock.system(timeRange.zoneId)

  def withSaveMode(sm: SaveMode): SparKafkaParams = copy(saveMode = sm)
  def withOverwrite: SparKafkaParams              = copy(saveMode = SaveMode.Overwrite)

  def withPathBuilder(rp: KafkaPathBuild => String): SparKafkaParams =
    copy(pathBuilder = Reader(rp))

  def withFileFormat(ff: NJFileFormat): SparKafkaParams = copy(fileFormat = ff)

  def withJson: SparKafkaParams    = withFileFormat(NJFileFormat.Json)
  def withJackson: SparKafkaParams = withFileFormat(NJFileFormat.Jackson)
  def withAvro: SparKafkaParams    = withFileFormat(NJFileFormat.Avro)
  def withParquet: SparKafkaParams = withFileFormat(NJFileFormat.Parquet)

  def withLocationStrategy(ls: LocationStrategy): SparKafkaParams =
    copy(locationStrategy = ls)

  def withBatchSize(batchSize: Int): SparKafkaParams =
    SparKafkaParams.uploadRate.composeLens(NJRate.batchSize).set(batchSize)(this)

  def withDuration(duration: FiniteDuration): SparKafkaParams =
    SparKafkaParams.uploadRate.composeLens(NJRate.duration).set(duration)(this)

  def withUploadRate(batchSize: Int, duration: FiniteDuration): SparKafkaParams =
    withBatchSize(batchSize).withDuration(duration)

  def withSparkRepartition(number: Int): SparKafkaParams =
    SparKafkaParams.repartition.set(number)(this)
}

object SparKafkaParams {

  val default: SparKafkaParams =
    SparKafkaParams(
      timeRange         = NJDateTimeRange.infinite,
      conversionTactics = ConversionTactics.default,
      uploadRate        = NJRate.default,
      pathBuilder       = Reader(ps => s"./data/spark/kafka/${ps.topicName}/${ps.fileFormat}/"),
      fileFormat        = NJFileFormat.Parquet,
      saveMode          = SaveMode.ErrorIfExists,
      locationStrategy  = LocationStrategies.PreferConsistent,
      repartition       = 30
    )
}
