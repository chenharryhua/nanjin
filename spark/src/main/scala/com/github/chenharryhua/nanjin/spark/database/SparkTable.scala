package com.github.chenharryhua.nanjin.spark.database

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.database.{DatabaseName, DatabaseSettings, TableName}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.loaders
import frameless.TypedDataset
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

final class SparkTable[F[_], A](
  val tableDef: TableDef[A],
  val dbSettings: DatabaseSettings,
  cfg: STConfig)(implicit sparkSession: SparkSession)
    extends Serializable {

  implicit private val ate: AvroTypedEncoder[A] = tableDef.avroTypedEncoder

  val params: STParams = cfg.evalConfig

  val tableName: TableName = tableDef.tableName

  def withQuery(query: String): SparkTable[F, A] =
    new SparkTable[F, A](tableDef, dbSettings, cfg.withQuery(query))

  def withReplayPathBuilder(f: (DatabaseName, TableName) => String): SparkTable[F, A] =
    new SparkTable[F, A](tableDef, dbSettings, cfg.withReplayPathBuilder(f))

  def fromDB: TableDataset[F, A] = {
    val df =
      sd.unloadDF(
        dbSettings.hikariConfig,
        tableDef.tableName,
        params.query.orElse(tableDef.unloadQuery))
    new TableDataset[F, A](ate.normalizeDF(df).dataset, dbSettings, cfg)
  }

  def fromDisk: TableDataset[F, A] =
    new TableDataset[F, A](
      loaders.objectFile(params.replayPath, tableDef.avroTypedEncoder).dataset,
      dbSettings,
      cfg)

  def dump(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    Blocker[F].use(blocker => fromDB.save.overwrite.objectFile(params.replayPath).run(blocker))

  def tableset(ds: Dataset[A]): TableDataset[F, A] =
    new TableDataset[F, A](ate.normalize(ds).dataset, dbSettings, cfg)

  def tableset(tds: TypedDataset[A]): TableDataset[F, A] =
    new TableDataset[F, A](ate.normalize(tds).dataset, dbSettings, cfg)

  def tableset(rdd: RDD[A]): TableDataset[F, A] =
    new TableDataset[F, A](ate.normalize(rdd).dataset, dbSettings, cfg)

}
