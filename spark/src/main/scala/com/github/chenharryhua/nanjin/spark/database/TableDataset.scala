package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.database.{DatabaseName, DatabaseSettings, TableName}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist._
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.apache.spark.sql.{Dataset, SparkSession}

final class TableDataset[F[_], A](ds: Dataset[A], dbSettings: DatabaseSettings, cfg: STConfig)(
  implicit ate: AvroTypedEncoder[A])
    extends Serializable {

  import ate.sparkTypedEncoder.classTag
  implicit private val ss: SparkSession   = ds.sparkSession
  implicit private val ae: NJAvroCodec[A] = ate.avroCodec

  val params: STParams = cfg.evalConfig

  def repartition(num: Int): TableDataset[F, A] =
    new TableDataset[F, A](ds.repartition(num), dbSettings, cfg)

  def withPathBuilder(f: (DatabaseName, TableName, NJFileFormat) => String) =
    new TableDataset[F, A](ds, dbSettings, cfg.withPathBuilder(f))

  def typedDataset: TypedDataset[A] = ate.normalize(ds)

  def upload: DbUploader[F, A] = new DbUploader[F, A](ds, dbSettings, ate, cfg)

  def save: RddFileSaver[F, A] = new RddFileSaver[F, A](ds.rdd)

}
