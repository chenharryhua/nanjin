package com.github.chenharryhua.nanjin.spark.database

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.database.TableName
import com.github.chenharryhua.nanjin.database.DatabaseSettings
import org.apache.spark.sql.{Dataset, SaveMode}

final class DbUploader[F[_], A](ds: Dataset[A], dbSettings: DatabaseSettings, cfg: STConfig) extends Serializable {

  val params: STParams = cfg.evalConfig

  private def mode(sm: SaveMode): DbUploader[F, A] =
    new DbUploader[F, A](ds, dbSettings, cfg.save_mode(sm))

  def overwrite: DbUploader[F, A]      = mode(SaveMode.Overwrite)
  def append: DbUploader[F, A]         = mode(SaveMode.Append)
  def ignoreIfExists: DbUploader[F, A] = mode(SaveMode.Ignore)
  def errorIfExists: DbUploader[F, A]  = mode(SaveMode.ErrorIfExists)

  def withTableName(tableName: String): DbUploader[F, A] =
    new DbUploader[F, A](ds, dbSettings, cfg.table_name(TableName.unsafeFrom(tableName)))

  def run(implicit F: Sync[F]): F[Unit] =
    F.delay {
      ds.write
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
