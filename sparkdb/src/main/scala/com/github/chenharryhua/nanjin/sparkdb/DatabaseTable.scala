package com.github.chenharryhua.nanjin.sparkdb
import cats.effect.{Concurrent, ContextShift, Resource}
import doobie.Fragment
import doobie.free.connection.ConnectionIO
import doobie.hikari.HikariTransactor
import doobie.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import org.apache.spark.sql.{SaveMode, SparkSession}

final case class TableDef[A](schema: String, table: String)(
  implicit
  val typedEncoder: TypedEncoder[A],
  val doobieReadA: doobie.Read[A]) {

  val tableName: String = s"$schema.$table"

  def in[F[_]: ContextShift: Concurrent](dbSettings: DatabaseSettings): DatabaseTable[F, A] =
    DatabaseTable[F, A](this, dbSettings)
}

final case class DatabaseTable[F[_]: ContextShift: Concurrent, A](
  tableDef: TableDef[A],
  dbSettings: DatabaseSettings) {
  import tableDef.{doobieReadA, typedEncoder}

  private val transactor: Resource[F, HikariTransactor[F]] = dbSettings.transactor[F]

  def dataset(implicit spark: SparkSession): TypedDataset[A] =
    TypedDataset.createUnsafe[A](
      spark.read
        .format("jdbc")
        .option("url", dbSettings.connStr.value)
        .option("driver", dbSettings.driver.value)
        .option("dbtable", tableDef.tableName)
        .load())

  val source: Stream[F, A] =
    for {
      xa <- Stream.resource(transactor)
      dt: Stream[ConnectionIO, A] = (fr"select * from" ++ Fragment.const(tableDef.tableName))
        .query[A]
        .stream
      rst <- xa.transP.apply(dt)
    } yield rst

  private def uploadToDB(data: TypedDataset[A], saveMode: SaveMode): Unit =
    data.write
      .mode(saveMode)
      .format("jdbc")
      .option("url", dbSettings.connStr.value)
      .option("driver", dbSettings.driver.value)
      .option("dbtable", tableDef.tableName)
      .save()

  def appendDB(data: TypedDataset[A]): Unit = uploadToDB(data, SaveMode.Append)

  def overwriteDB(data: TypedDataset[A]): Unit = uploadToDB(data, SaveMode.Overwrite)

}
