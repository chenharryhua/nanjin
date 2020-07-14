package mtest.spark.database

import java.sql.{Date, Timestamp}
import java.time._

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.datetime.transformers._
import com.github.chenharryhua.nanjin.database.TableName
import com.github.chenharryhua.nanjin.spark.database._
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.TypedDataset
import frameless.cats.implicits._
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.SaveMode
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
  implicit val zoneId: ZoneId  = beijingTime
  val table: TableDef[DBTable] = TableDef[DBTable](TableName("public.sparktabletest"))

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
    val data = TypedDataset.create(List(sample.transformInto[DBTable]))
    data.dbUpload(table.in(db).withParamUpdate(_.withDbSaveMode(SaveMode.Overwrite)))
  }

  test("sparkTable save db table to disk") {
    table.in(db).save()
  }

  test("sparkTable read table on disk") {
    val rst: DomainObject =
      table.in(db).fromDisk.collect[IO].map(_.head.transformInto[DomainObject]).unsafeRunSync
    assert(rst.==(sample))
  }
}
