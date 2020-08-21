package com.github.chenharryhua.nanjin.spark.database

import cats.effect.Sync
import com.github.chenharryhua.nanjin.database.DatabaseSettings
import com.github.chenharryhua.nanjin.spark.RddExt
import com.sksamuel.avro4s.{Encoder, SchemaFor}
import org.apache.avro.Schema
import org.apache.spark.sql.{Dataset, SaveMode}

final class DbUploader[F[_], A](
  ds: Dataset[A],
  dbSettings: DatabaseSettings,
  avroEncoder: Encoder[A],
  cfg: STConfig)
    extends Serializable {

  val params: STParams = cfg.evalConfig

  private def mode(sm: SaveMode): DbUploader[F, A] =
    new DbUploader[F, A](ds, dbSettings, avroEncoder, cfg.withDbSaveMode(sm))

  def overwrite: DbUploader[F, A]      = mode(SaveMode.Overwrite)
  def append: DbUploader[F, A]         = mode(SaveMode.Append)
  def ignoreIfExists: DbUploader[F, A] = mode(SaveMode.Ignore)
  def errorIfExists: DbUploader[F, A]  = mode(SaveMode.ErrorIfExists)

  def withEncoder(avroEncoder: Encoder[A]): DbUploader[F, A] =
    new DbUploader[F, A](ds, dbSettings, avroEncoder, cfg)

  def withSchema(schema: Schema): DbUploader[F, A] =
    new DbUploader[F, A](ds, dbSettings, avroEncoder.withSchema(SchemaFor[A](schema)), cfg)

  def run(implicit F: Sync[F]): F[Unit] =
    F.delay {
      ds.rdd
        .toDF(avroEncoder, ds.sparkSession)
        .write
        .mode(params.dbSaveMode)
        .format("jdbc")
        .option("url", dbSettings.connStr.value)
        .option("driver", dbSettings.driver.value)
        .option("dbtable", params.tableName.value)
        .save()
    }
}
