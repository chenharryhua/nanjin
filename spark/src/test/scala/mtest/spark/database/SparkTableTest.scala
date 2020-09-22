package mtest.spark.database

import java.sql.Date
import java.time._

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.common.transformers._
import com.github.chenharryhua.nanjin.database.TableName
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.database._
import com.github.chenharryhua.nanjin.spark.injection._
import doobie.implicits._
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import io.circe.generic.auto._
import io.scalaland.chimney.dsl._
import kantan.csv.RowEncoder
import kantan.csv.generic._
import kantan.csv.java8._
import org.scalatest.funsuite.AnyFunSuite

final case class DomainObject(
  a: LocalDate,
  b: Date,
  c: ZonedDateTime,
  d: OffsetDateTime,
  e: Instant)

final case class DBTable(a: LocalDate, b: LocalDate, c: Instant, d: Instant, e: Instant)

object DBTable {

  val drop: doobie.ConnectionIO[Int] =
    sql"DROP TABLE IF EXISTS sparktest".update.run

  val create: doobie.ConnectionIO[Int] =
    sql"""
         CREATE TABLE sparktest (
             a  date,
             b  date,
             c  timestamp,
             d  timestamp,
             e  timestamp
         );""".update.run
}

class SparkTableTest extends AnyFunSuite {
  implicit val zoneId: ZoneId = beijingTime

  val codec: AvroCodec[DBTable]          = AvroCodec[DBTable]
  implicit val te: TypedEncoder[DBTable] = shapeless.cachedImplicit
  implicit val re: RowEncoder[DBTable]   = shapeless.cachedImplicit

  val table: TableDef[DBTable] = TableDef[DBTable](TableName("sparktest"), codec)

  val sample: DomainObject =
    DomainObject(
      LocalDate.now,
      Date.valueOf(LocalDate.now),
      ZonedDateTime.now(zoneId),
      OffsetDateTime.now(zoneId),
      Instant.now)

  val dbData: DBTable = sample.transformInto[DBTable]

  postgres.runQuery[IO, Int](blocker, DBTable.drop *> DBTable.create).unsafeRunSync()

  test("sparkTable upload dataset to table") {
    val data = TypedDataset.create(List(sample.transformInto[DBTable])).dataset

    sparkSession
      .alongWith[IO](postgres)
      .table(table)
      .tableset(data)
      .upload
      .overwrite
      .run
      .unsafeRunSync()
  }

  test("partition save") {
    val run = table.in[IO](postgres).fromDB.partition.jackson.run(blocker) >>
      table.in[IO](postgres).fromDB.partition.avro.run(blocker) >>
      table.in[IO](postgres).fromDB.partition.parquet.run(blocker) >>
      table.in[IO](postgres).fromDB.partition.circe.run(blocker) >>
      table.in[IO](postgres).fromDB.partition.csv.run(blocker) >>
      IO(())
    run.unsafeRunSync
  }
  val root = "./data/test/spark/database/postgres/"

  val tb: SparkTable[IO, DBTable] = table.in[IO](postgres)

  val saver = tb.fromDB.save

  test("avro") {
    val avro = saver.avro(root + "single.raw.avro").file.run(blocker) >>
      saver.avro(root + "multi.raw.avro").folder.run(blocker)
    avro.unsafeRunSync()
    assert(table.load.avro(root + "single.raw.avro").dataset.collect.head == dbData)
    assert(table.load.avro(root + "multi.raw.avro").dataset.collect.head == dbData)
  }
  test("parquet") {
    val parquet = saver.parquet(root + "multi.parquet").run(blocker)
    parquet.unsafeRunSync()
    assert(table.load.parquet(root + "multi.parquet").dataset.collect.head == dbData)
  }
  test("circe") {
    val circe = saver.circe(root + "multi.circe.json").folder.run(blocker) >>
      saver.circe(root + "single.circe.json").file.run(blocker)
    circe.unsafeRunSync()
    assert(table.load.circe(root + "multi.circe.json").dataset.collect.head == dbData)
    assert(table.load.circe(root + "single.circe.json").dataset.collect.head == dbData)
  }

  test("csv") {
    val csv = saver.csv(root + "multi.csv").folder.run(blocker) >>
      saver.csv(root + "single.csv").file.run(blocker)
    csv.unsafeRunSync()
    assert(table.load.csv(root + "multi.csv").dataset.collect.head == dbData)
    assert(table.load.csv(root + "single.csv").dataset.collect.head == dbData)
  }
  test("spark json") {
    val json = saver.json(root + "spark.json").run(blocker)
    json.unsafeRunSync()
    assert(table.load.json(root + "spark.json").dataset.collect.head == dbData)
  }
}
