package mtest.spark.database

import java.sql.Date
import java.time._

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.common.transformers._
import com.github.chenharryhua.nanjin.database.TableName
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.spark.database._
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.TypedDataset
import frameless.cats.implicits._
import io.circe.generic.auto._
import io.scalaland.chimney.dsl._
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

class SparkTableTest extends AnyFunSuite {
  implicit val zoneId: ZoneId = beijingTime

  val table: SparkTable[IO, DBTable] =
    TableDef[DBTable](TableName("public.sparktabletest")).in[IO](postgres).overwrite

  val sample: DomainObject =
    DomainObject(
      LocalDate.now,
      Date.valueOf(LocalDate.now),
      ZonedDateTime.now(zoneId),
      OffsetDateTime.now(zoneId),
      Instant.now)

  test("sparkTable upload dataset to table") {
    val data = TypedDataset.create(List(sample.transformInto[DBTable])).dataset.rdd
    data.dbUpload(table).unsafeRunSync()
  }

  val path = "./data/test/spark/database/jackson.json"

  test("sparkTable save db table to disk") {
    table.fromDB.save.jackson(path).single.run(blocker).unsafeRunSync()
  }

  test("sparkTable read table on disk") {
    val rst: DomainObject =
      table.load
        .jackson(path)
        .typedDataset
        .collect[IO]
        .unsafeRunSync()
        .head
        .transformInto[DomainObject]
    assert(rst == sample)
  }

  test("partition save") {
    val run = table.fromDB.save.partition.jackson.run(blocker) >>
      table.fromDB.save.partition.avro.run(blocker) >>
      table.fromDB.save.partition.parquet.run(blocker) >>
      table.fromDB.save.partition.circe.run(blocker) >>
      table.fromDB.save.partition.csv.run(blocker) >>
      IO(())
    run.unsafeRunSync
  }
  test("save") {
    val run = table.fromDB.save.jackson.single.run(blocker) >>
      table.fromDB.save.avro.single.run(blocker) >>
      table.fromDB.save.parquet.single.run(blocker) >>
      table.fromDB.save.circe.single.run(blocker) >>
      table.fromDB.save.csv.single.run(blocker) >>
      IO(())
    run.unsafeRunSync
  }
  test("with query") {
    table
      .withQuery(s"select * from ${table.tableName.value}")
      .fromDB
      .save
      .jackson
      .single
      .run(blocker)
  }
}
