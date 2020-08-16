package mtest.spark.database

import java.sql.{Date, Timestamp}
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
import io.scalaland.chimney.dsl._
import org.scalatest.funsuite.AnyFunSuite

final case class DomainObject(
  a: LocalDate,
  b: LocalDate,
  c: Date,
  d: Date,
  e: ZonedDateTime,
  f: OffsetDateTime,
  g: Instant,
  h: LocalDateTime,
  i: Timestamp)

final case class DBTable(
  a: Date,
  b: LocalDate,
  c: Date,
  d: LocalDate,
  e: Instant,
  f: Instant,
  g: Instant,
  h: Timestamp,
  i: Timestamp)

class SparkTableTest extends AnyFunSuite {
  implicit val zoneId: ZoneId = beijingTime

  val table: SparkTable[IO, DBTable] =
    TableDef[DBTable](TableName("public.sparktabletest")).in[IO](postgres).overwrite

  val sample: DomainObject =
    DomainObject(
      LocalDate.now,
      LocalDate.now,
      Date.valueOf(LocalDate.now),
      Date.valueOf(LocalDate.now),
      ZonedDateTime.now(zoneId),
      OffsetDateTime.now(zoneId),
      Instant.now,
      LocalDateTime.now(zoneId),
      new Timestamp(1351245600000L)
    )

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
}
