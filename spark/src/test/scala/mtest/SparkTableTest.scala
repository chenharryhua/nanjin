package mtest

import java.time.{Instant, LocalDate, LocalDateTime}

import cats.effect.IO
import com.github.chenharryhua.nanjin.database._
import com.github.chenharryhua.nanjin.database.meta._
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.datetime.iso._
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.database._
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.TypedDataset
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite
import frameless.cats.implicits._
import java.time.ZoneId

import com.github.chenharryhua.nanjin.spark.database.TableDef
final case class DbTableInst(a: LocalDate, b: LocalDateTime, c: Int, d: String, e: Instant)

class SparkTableTest extends AnyFunSuite {
  implicit val zoneId = ZoneId.systemDefault()

  val table = TableDef[DbTableInst]("public.sparktabletest")

  test("upload dataset to table") {
    val data =
      TypedDataset.create(List(DbTableInst(LocalDate.now, LocalDateTime.now, 10, "d", Instant.now)))
    data.dbUpload(table.in(db).updateParams(_.withDBSaveMode(SaveMode.Overwrite))).unsafeRunSync
  }

  test("save db table to disk") {
    table.in(db).saveToDisk.unsafeRunSync
  }

  test("read table on disk") {
    table.in(db).datasetFromDisk.show[IO]().unsafeRunSync
  }
}
