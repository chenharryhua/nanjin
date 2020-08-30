package com.github.chenharryhua.nanjin.spark.database

import cats.Show
import cats.effect.Sync
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.database.{DatabaseName, DatabaseSettings, TableName}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist._
import com.sksamuel.avro4s.{Decoder => AvroDecoder, Encoder => AvroEncoder}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import frameless.{TypedDataset, TypedEncoder}
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.RowEncoder
import org.apache.spark.sql.Dataset

final class TableDataset[F[_], A](ds: Dataset[A], dbSettings: DatabaseSettings, cfg: STConfig)(
  implicit ate: AvroTypedEncoder[A])
    extends Serializable {

  import ate.sparkTypedEncoder.classTag

  val params: STParams = cfg.evalConfig

  def repartition(num: Int): TableDataset[F, A] =
    new TableDataset[F, A](ds.repartition(num), dbSettings, cfg)

  def withPathBuilder(f: (DatabaseName, TableName, NJFileFormat) => String) =
    new TableDataset[F, A](ds, dbSettings, cfg.withPathBuilder(f))

  def typedDataset: TypedDataset[A] = ate.fromDS(ds)

  def upload: DbUploader[F, A] = new DbUploader[F, A](ds, dbSettings, ate, cfg)

}
