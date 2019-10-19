package mtest

import java.time.{LocalDate, LocalDateTime}

import cats.effect.IO
import com.github.chenharryhua.nanjin.sparkdb._
import com.github.chenharryhua.nanjin.spark._
import frameless.TypedDataset
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite
import frameless.cats.implicits._
final case class DbTableInst(a: LocalDate, b: LocalDateTime, c: Int, d: String)

class SparkTableTest extends AnyFunSuite {
  val table = TableDef[DbTableInst]("public.sparktabletest")

  test("upload dataset to table") {
    sparkSession.use { implicit s =>
      val data = TypedDataset.create(List(DbTableInst(LocalDate.now, LocalDateTime.now, 10, "d")))
      data.dbUpload(table.in(db).update(_.withDBSaveMode(SaveMode.Overwrite)))
    }.unsafeRunSync
  }

  test("save db table to disk") {
    sparkSession.use { implicit s =>
      table.in(db).saveToDisk
    }.unsafeRunSync
  }

  test("read table on disk") {
    sparkSession.use { implicit s =>
      table.in(db).datasetFromDisk.show[IO]()
    }.unsafeRunSync
  }
}
