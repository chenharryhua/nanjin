package com.github.chenharryhua.nanjin.spark.kafka

import java.time._

import cats.data.Reader
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.TopicName
import com.github.chenharryhua.nanjin.spark.{NJRepartition, NJShowDataset}
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka010.{LocationStrategies, LocationStrategy}

import scala.concurrent.duration._

@Lenses final case class UploadRate(batchSize: Int, duration: FiniteDuration)

object UploadRate {
  val default: UploadRate = UploadRate(batchSize = 1000, duration = 1.second)
}

final case class KafkaPathBuild(
  timeRange: NJDateTimeRange,
  fileFormat: NJFileFormat,
  topicName: TopicName)

@Lenses final case class SparKafkaParams private (
  timeRange: NJDateTimeRange,
  uploadRate: UploadRate,
  pathBuilder: Reader[KafkaPathBuild, String],
  fileFormat: NJFileFormat,
  saveMode: SaveMode,
  locationStrategy: LocationStrategy,
  repartition: NJRepartition,
  showDs: NJShowDataset)

object SparKafkaParams {

  val default: SparKafkaParams =
    SparKafkaParams(
      timeRange         = NJDateTimeRange.infinite,
      uploadRate        = UploadRate.default,
      pathBuilder       = Reader(kpb => s"./data/spark/kafka/${kpb.topicName}/${kpb.fileFormat}/"),
      fileFormat        = NJFileFormat.Parquet,
      saveMode          = SaveMode.ErrorIfExists,
      locationStrategy  = LocationStrategies.PreferConsistent,
      repartition       = NJRepartition(30),
      showDs            = NJShowDataset(60, isTruncate = false)
    )
}

@Lenses final case class KitBundle[K, V](kit: KafkaTopicKit[K, V], params: SparKafkaParams) {

  def getPath: String =
    params.pathBuilder(KafkaPathBuild(params.timeRange, params.fileFormat, kit.topicName))

  def withTimeRange(f: NJDateTimeRange => NJDateTimeRange): KitBundle[K, V] =
    KitBundle.params.composeLens(SparKafkaParams.timeRange).modify(f)(this)

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
  def withText: KitBundle[K, V]    = withFileFormat(NJFileFormat.Text)

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
    KitBundle.params.composeLens(SparKafkaParams.repartition).set(NJRepartition(num))(this)

  def withRowNumber(num: Int): KitBundle[K, V] =
    KitBundle.params
      .composeLens(SparKafkaParams.showDs)
      .composeLens(NJShowDataset.rowNum)
      .set(num)(this)

  def withTruncate: KitBundle[K, V] =
    KitBundle.params
      .composeLens(SparKafkaParams.showDs)
      .composeLens(NJShowDataset.isTruncate)
      .set(true)(this)

  def withoutTruncate: KitBundle[K, V] =
    KitBundle.params
      .composeLens(SparKafkaParams.showDs)
      .composeLens(NJShowDataset.isTruncate)
      .set(false)(this)
}
