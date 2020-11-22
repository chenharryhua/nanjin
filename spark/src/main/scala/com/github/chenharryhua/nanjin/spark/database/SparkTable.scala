package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.database.{DatabaseName, DatabaseSettings, TableName}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import frameless.TypedDataset
import org.apache.spark.sql.{Dataset, SparkSession}

final class SparkTable[F[_], A](
  tableDef: TableDef[A],
  dbSettings: DatabaseSettings,
  cfg: STConfig,
  ss: SparkSession)
    extends Serializable {

  implicit private val ate: AvroTypedEncoder[A] = tableDef.avroTypedEncoder

  implicit private val sparkSession: SparkSession = ss

  val params: STParams = cfg.evalConfig

  val tableName: TableName = tableDef.tableName

  def withQuery(query: String): SparkTable[F, A] =
    new SparkTable[F, A](tableDef, dbSettings, cfg.withQuery(query), ss)

  def withPathBuilder(f: (DatabaseName, TableName, NJFileFormat) => String): SparkTable[F, A] =
    new SparkTable[F, A](tableDef, dbSettings, cfg.withPathBuilder(f), ss)

  def fromDB: TableDataset[F, A] = {
    val df =
      sd.unloadDF(
        dbSettings.hikariConfig,
        tableDef.tableName,
        params.query.orElse(tableDef.unloadQuery))
    new TableDataset[F, A](ate.normalizeDF(df).dataset, dbSettings, cfg)
  }

  def tableset(ds: Dataset[A]): TableDataset[F, A] =
    new TableDataset[F, A](ate.normalize(ds).dataset, dbSettings, cfg)

  def tableset(tds: TypedDataset[A]): TableDataset[F, A] =
    new TableDataset[F, A](ate.normalize(tds).dataset, dbSettings, cfg)

}
