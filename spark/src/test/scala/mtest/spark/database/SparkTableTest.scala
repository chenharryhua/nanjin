package mtest.spark.database

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, ZoneId, ZonedDateTime}

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.database.TableName
import com.github.chenharryhua.nanjin.spark.database._
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.datetime._
import frameless.TypedDataset
import frameless.cats.implicits._
import io.scalaland.chimney.dsl._
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite

final case class DomainObject(a: LocalDate, b: ZonedDateTime, c: Int, d: String, e: Instant)

final case class DbTableInstDB(a: LocalDate, b: Timestamp, c: Int, d: String, e: Timestamp)

class SparkTableTest extends AnyFunSuite {
  val table: TableDef[DbTableInstDB] = TableDef[DbTableInstDB](TableName("public.sparktabletest"))

  val sample: DomainObject = DomainObject(LocalDate.now, ZonedDateTime.now, 10, "d", Instant.now)

  test("upload dataset to table") {
    val data = TypedDataset.create(List(sample.transformInto[DbTableInstDB]))
    data.dbUpload(table.in(db).withParamUpdate(_.withDbSaveMode(SaveMode.Overwrite)))
  }

  test("save db table to disk") {
    table.in(db).save()
  }

  test("read table on disk") {
    val rst: DomainObject =
      table
        .in(db)
        .fromDisk
        .collect[IO]
        .map(
          _.head
            .into[DomainObject]
            .withFieldComputed(
              _.b,
              db => ZonedDateTime.ofInstant(db.b.toInstant, ZoneId.systemDefault()))
            .transform)
        .unsafeRunSync
    assert(rst.==(sample))
  }
}
