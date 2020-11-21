package com.github.chenharryhua.nanjin.spark.database

import cats.effect.Sync
import com.github.chenharryhua.nanjin.database.{DatabaseSettings, TableName}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import org.apache.spark.sql.{Dataset, SaveMode}

final class DbUploader[F[_], A](
  ds: Dataset[A],
  dbSettings: DatabaseSettings,
  ate: AvroTypedEncoder[A],
  cfg: STConfig)
    extends Serializable {

  val params: STParams = cfg.evalConfig

  private def mode(sm: SaveMode): DbUploader[F, A] =
    new DbUploader[F, A](ds, dbSettings, ate, cfg.withDbSaveMode(sm))

  def overwrite: DbUploader[F, A]      = mode(SaveMode.Overwrite)
  def append: DbUploader[F, A]         = mode(SaveMode.Append)
  def ignoreIfExists: DbUploader[F, A] = mode(SaveMode.Ignore)
  def errorIfExists: DbUploader[F, A]  = mode(SaveMode.ErrorIfExists)

  def withTableName(tableName: String): DbUploader[F, A] =
    new DbUploader[F, A](ds, dbSettings, ate, cfg.withTableName(TableName.unsafeFrom(tableName)))

  def run(implicit F: Sync[F]): F[Unit] =
    F.delay {
      ate
        .normalize(ds)
        .write
        .mode(params.dbSaveMode)
        .format("jdbc")
        .option("driver", dbSettings.hikariConfig.getDriverClassName)
        .option("url", dbSettings.hikariConfig.getJdbcUrl)
        .option("user", dbSettings.hikariConfig.getUsername)
        .option("password", dbSettings.hikariConfig.getPassword)
        .option("dbtable", params.tableName.value)
        .save()
    }
}
