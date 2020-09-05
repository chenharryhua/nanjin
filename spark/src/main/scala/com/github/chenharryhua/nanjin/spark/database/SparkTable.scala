package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.database.{DatabaseName, DatabaseSettings, TableName}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import org.apache.spark.sql.{Dataset, SparkSession}

final class SparkTable[F[_], A](
  tableDef: TableDef[A],
  dbSettings: DatabaseSettings,
  cfg: STConfig,
  ss: SparkSession)
    extends Serializable {

  private val ate: AvroTypedEncoder[A] = tableDef.encoder

  implicit private val sparkSession: SparkSession = ss

  val params: STParams = cfg.evalConfig

  val tableName: TableName = tableDef.tableName

  def withQuery(query: String): SparkTable[F, A] =
    new SparkTable[F, A](tableDef, dbSettings, cfg.withQuery(query), ss)

  def withPathBuilder(f: (DatabaseName, TableName, NJFileFormat) => String): SparkTable[F, A] =
    new SparkTable[F, A](tableDef, dbSettings, cfg.withPathBuilder(f), ss)

  def fromDB: TableDataset[F, A] = {
    val df = sd.unloadDF(dbSettings.connStr, dbSettings.driver, tableDef.tableName, params.query)
    new TableDataset[F, A](ate.normalizeDF(df).dataset, dbSettings, cfg)(ate)
  }

  def tableset(ds: Dataset[A]): TableDataset[F, A] =
    new TableDataset[F, A](ate.normalize(ds).dataset, dbSettings, cfg)(ate)

}
