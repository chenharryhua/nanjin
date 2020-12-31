package com.github.chenharryhua.nanjin.spark.database

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.database.{DatabaseName, DatabaseSettings, TableName}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.loaders
import frameless.{TypedDataset, TypedExpressionEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

final class SparkDBTable[F[_], A](
  val tableDef: TableDef[A],
  val dbSettings: DatabaseSettings,
  val cfg: STConfig,
  val sparkSession: SparkSession)
    extends Serializable {

  private val ate: AvroTypedEncoder[A] = tableDef.avroTypedEncoder

  val params: STParams = cfg.evalConfig

  val tableName: TableName = tableDef.tableName

  def withQuery(query: String): SparkDBTable[F, A] =
    new SparkDBTable[F, A](tableDef, dbSettings, cfg.withQuery(query), sparkSession)

  def withReplayPathBuilder(f: (DatabaseName, TableName) => String): SparkDBTable[F, A] =
    new SparkDBTable[F, A](tableDef, dbSettings, cfg.withReplayPathBuilder(f), sparkSession)

  def fromDB: TableDS[F, A] = {
    val df =
      sd.unloadDF(dbSettings.hikariConfig, tableDef.tableName, params.query.orElse(tableDef.unloadQuery), sparkSession)
    new TableDS[F, A](ate.normalizeDF(df).dataset, tableDef, dbSettings, cfg)
  }

  def fromDisk: TableDS[F, A] =
    new TableDS[F, A](loaders.objectFile(params.replayPath, ate, sparkSession).dataset, tableDef, dbSettings, cfg)

  def countDisk: Long = fromDisk.dataset.count

  def countDB: Long =
    sd.unloadDF(
      dbSettings.hikariConfig,
      tableDef.tableName,
      Some(s"select count(*) from ${tableDef.tableName.value}"),
      sparkSession)
      .as[Long](TypedExpressionEncoder[Long])
      .head()

  def dump(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    Blocker[F].use(blocker => fromDB.save.objectFile(params.replayPath).overwrite.run(blocker))

  def tableset(ds: Dataset[A]): TableDS[F, A] =
    new TableDS[F, A](ate.normalize(ds).dataset, tableDef, dbSettings, cfg)

  def tableset(tds: TypedDataset[A]): TableDS[F, A] =
    new TableDS[F, A](ate.normalize(tds).dataset, tableDef, dbSettings, cfg)

  def tableset(rdd: RDD[A]): TableDS[F, A] =
    new TableDS[F, A](ate.normalize(rdd, sparkSession).dataset, tableDef, dbSettings, cfg)

  def load: LoadTableFile[F, A] = new LoadTableFile[F, A](this)

}
