package com.github.chenharryhua.nanjin.spark.kafka

import java.time._

import cats.data.Reader
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.TopicName
import com.github.chenharryhua.nanjin.spark.{Repartition, ShowSparkDataset}
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka010.{LocationStrategies, LocationStrategy}

import scala.concurrent.duration._

@Lenses final case class UploadRate(batchSize: Int, duration: FiniteDuration)

object UploadRate {
  val default: UploadRate = UploadRate(batchSize = 1000, duration = 1.second)
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
  uploadRate: UploadRate,
  pathBuilder: Reader[KafkaPathBuild, String],
  fileFormat: NJFileFormat,
  saveMode: SaveMode,
  locationStrategy: LocationStrategy,
  repartition: Repartition,
  showDs: ShowSparkDataset)

object SparKafkaParams {

  val default: SparKafkaParams =
    SparKafkaParams(
      timeRange         = NJDateTimeRange.infinite,
      conversionTactics = ConversionTactics.default,
      uploadRate        = UploadRate.default,
      pathBuilder       = Reader(ps => s"./data/spark/kafka/${ps.topicName}/${ps.fileFormat}/"),
      fileFormat        = NJFileFormat.Parquet,
      saveMode          = SaveMode.ErrorIfExists,
      locationStrategy  = LocationStrategies.PreferConsistent,
      repartition       = Repartition(30),
      showDs            = ShowSparkDataset(60, isTruncate = false)
    )
}

@Lenses final case class KitBundle[K, V](kit: KafkaTopicKit[K, V], params: SparKafkaParams) {

  def getPath: String =
    params.pathBuilder(KafkaPathBuild(params.timeRange, params.fileFormat, kit.topicName))

  def withTimeRange(f: NJDateTimeRange => NJDateTimeRange): KitBundle[K, V] =
    KitBundle.params.composeLens(SparKafkaParams.timeRange).modify(f)(this)

  def withConversionTactics(f: ConversionTactics => ConversionTactics): KitBundle[K, V] =
    KitBundle.params.composeLens(SparKafkaParams.conversionTactics).modify(f)(this)

  def zoneId: ZoneId = params.timeRange.zoneId
  def clock: Clock   = Clock.system(zoneId)

  def withSaveMode(sm: SaveMode): KitBundle[K, V] =
    KitBundle.params.composeLens(SparKafkaParams.saveMode).set(sm)(this)

  def withOverwrite: KitBundle[K, V] =
    KitBundle.params.composeLens(SparKafkaParams.saveMode).set(SaveMode.Overwrite)(this)

  def withPathBuilder(pb: KafkaPathBuild => String): KitBundle[K, V] =
    KitBundle.params.composeLens(SparKafkaParams.pathBuilder).set(Reader(pb))(this)

  def withFileFormat(ff: NJFileFormat): KitBundle[K, V] =
    KitBundle.params.composeLens(SparKafkaParams.fileFormat).set(ff)(this)

  def withJson: KitBundle[K, V]    = withFileFormat(NJFileFormat.Json)
  def withJackson: KitBundle[K, V] = withFileFormat(NJFileFormat.Jackson)
  def withAvro: KitBundle[K, V]    = withFileFormat(NJFileFormat.Avro)
  def withParquet: KitBundle[K, V] = withFileFormat(NJFileFormat.Parquet)

  def withLocationStrategy(ls: LocationStrategy): KitBundle[K, V] =
    KitBundle.params.composeLens(SparKafkaParams.locationStrategy).set(ls)(this)

  def withBatchSize(batchSize: Int): KitBundle[K, V] =
    KitBundle.params
      .composeLens(SparKafkaParams.uploadRate)
      .composeLens(UploadRate.batchSize)
      .set(batchSize)(this)

  def withDuration(duration: FiniteDuration): KitBundle[K, V] =
    KitBundle.params
      .composeLens(SparKafkaParams.uploadRate)
      .composeLens(UploadRate.duration)
      .set(duration)(this)

  def withRepartition(num: Int): KitBundle[K, V] =
    KitBundle.params.composeLens(SparKafkaParams.repartition).set(Repartition(num))(this)

  def withRowNumber(num: Int): KitBundle[K, V] =
    KitBundle.params
      .composeLens(SparKafkaParams.showDs)
      .composeLens(ShowSparkDataset.rowNum)
      .set(num)(this)

  def withTruncate: KitBundle[K, V] =
    KitBundle.params
      .composeLens(SparKafkaParams.showDs)
      .composeLens(ShowSparkDataset.isTruncate)
      .set(true)(this)

  def withoutTruncate: KitBundle[K, V] =
    KitBundle.params
      .composeLens(SparKafkaParams.showDs)
      .composeLens(ShowSparkDataset.isTruncate)
      .set(false)(this)
}
