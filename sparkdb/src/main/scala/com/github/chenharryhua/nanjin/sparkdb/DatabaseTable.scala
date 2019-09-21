package com.github.chenharryhua.nanjin.sparkdb
import cats.effect.{Concurrent, ContextShift, Resource}
import cats.implicits._
import doobie.Fragment
import doobie.hikari.HikariTransactor
import doobie.implicits._
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import io.getquill.codegen.jdbc.SimpleJdbcCodegen
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

  val transactor: Resource[F, HikariTransactor[F]] = dbSettings.transactor[F]

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
      as <- (fr"select * from" ++ Fragment.const(tableDef.tableName))
        .query[A]
        .stream
        .translateInterruptible(xa.trans)
    } yield as

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

  def genCaseClass: F[Unit] = transactor.use {
    _.configure { hikari =>
      Concurrent[F]
        .delay(new SimpleJdbcCodegen(() => hikari.getConnection, ""))
        .map(_.writeStrings.toList.mkString("\n"))
        .map(println)
    }
  }
}
