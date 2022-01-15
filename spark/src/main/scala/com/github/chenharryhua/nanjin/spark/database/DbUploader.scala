package com.github.chenharryhua.nanjin.spark.database

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.database.TableName
import com.zaxxer.hikari.HikariConfig
import org.apache.spark.sql.{Dataset, SaveMode}

final class DbUploader[F[_], A](ds: Dataset[A], hikariConfig: HikariConfig, cfg: STConfig) extends Serializable {

  val params: STParams = cfg.evalConfig

  private def mode(sm: SaveMode): DbUploader[F, A] =
    new DbUploader[F, A](ds, hikariConfig, cfg.saveMode(sm))

  def overwrite: DbUploader[F, A]      = mode(SaveMode.Overwrite)
  def append: DbUploader[F, A]         = mode(SaveMode.Append)
  def ignoreIfExists: DbUploader[F, A] = mode(SaveMode.Ignore)
  def errorIfExists: DbUploader[F, A]  = mode(SaveMode.ErrorIfExists)

  def withTableName(tableName: String): DbUploader[F, A] =
    new DbUploader[F, A](ds, hikariConfig, cfg.tableName(TableName.unsafeFrom(tableName)))

  def run(implicit F: Sync[F]): F[Unit] =
    F.delay {
      ds.write
        .mode(params.dbSaveMode)
        .format("jdbc")
        .option("driver", hikariConfig.getDriverClassName)
        .option("url", hikariConfig.getJdbcUrl)
        .option("user", hikariConfig.getUsername)
        .option("password", hikariConfig.getPassword)
        .option("dbtable", params.tableName.value)
        .save()
    }
}
