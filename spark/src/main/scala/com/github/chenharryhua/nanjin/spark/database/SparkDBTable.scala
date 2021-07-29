package com.github.chenharryhua.nanjin.spark.database

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.database.{DatabaseName, TableName}
import com.github.chenharryhua.nanjin.database.DatabaseSettings
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.loaders
import frameless.{TypedDataset, TypedExpressionEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

final class SparkDBTable[F[_], A](val tableDef: TableDef[A], dbs: DatabaseSettings, cfg: STConfig, ss: SparkSession)
    extends Serializable {

  val tableName: TableName = tableDef.tableName

  private val ate: AvroTypedEncoder[A] = tableDef.avroTypedEncoder

  val params: STParams = cfg.evalConfig

  def withQuery(query: String): SparkDBTable[F, A] =
    new SparkDBTable[F, A](tableDef, dbs, cfg.unload_query(query), ss)

  def withReplayPathBuilder(f: (DatabaseName, TableName) => String): SparkDBTable[F, A] =
    new SparkDBTable[F, A](tableDef, dbs, cfg.replay_path_builder(f), ss)

  def fromDB(implicit F: Sync[F]): F[TableDS[F, A]] = F.blocking {
    val df: DataFrame =
      sd.unloadDF(dbs.hikariConfig, tableDef.tableName, params.query.orElse(tableDef.unloadQuery), ss)
    new TableDS[F, A](ate.normalizeDF(df).dataset, tableDef, dbs, cfg)
  }

  def fromDisk(implicit F: Sync[F]): F[TableDS[F, A]] =
    F.blocking(new TableDS[F, A](loaders.objectFile(params.replayPath, ate, ss).dataset, tableDef, dbs, cfg))

  def countDisk(implicit F: Sync[F]): F[Long] = fromDisk.map(_.dataset.count)

  def countDB(implicit F: Sync[F]): F[Long] =
    F.blocking(
      sd.unloadDF(dbs.hikariConfig, tableDef.tableName, Some(s"select count(*) from ${tableDef.tableName.value}"), ss)
        .as[Long](TypedExpressionEncoder[Long])
        .head())

  def dump(implicit F: Sync[F]): F[Unit] =
    fromDB.flatMap(_.save.objectFile(params.replayPath).overwrite.run)

  def tableset(ds: Dataset[A]): TableDS[F, A] =
    new TableDS[F, A](ate.normalize(ds).dataset, tableDef, dbs, cfg)

  def tableset(tds: TypedDataset[A]): TableDS[F, A] =
    new TableDS[F, A](ate.normalize(tds).dataset, tableDef, dbs, cfg)

  def tableset(rdd: RDD[A]): TableDS[F, A] =
    new TableDS[F, A](ate.normalize(rdd, ss).dataset, tableDef, dbs, cfg)

  def load: LoadTableFile[F, A] = new LoadTableFile[F, A](tableDef, dbs, cfg, ss)

}
