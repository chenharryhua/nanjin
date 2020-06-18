package mtest.spark.kafka

import java.time.{Instant, LocalDate, LocalDateTime}

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.database.TableName
import com.github.chenharryhua.nanjin.spark.database._
import com.github.chenharryhua.nanjin.datetime.iso._
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.TypedDataset
import frameless.cats.implicits._
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite

final case class DbTableInst(a: LocalDate, b: LocalDateTime, c: Int, d: String, e: Instant)

class SparkTableTest extends AnyFunSuite {
  val table: TableDef[DbTableInst] = TableDef[DbTableInst](TableName("public.sparktabletest"))

  val sample: DbTableInst = DbTableInst(LocalDate.now, LocalDateTime.now, 10, "d", Instant.now)

  ignore("upload dataset to table") {
    val data = TypedDataset.create(List(sample))
    data.dbUpload(table.in(db).withParamUpdate(_.withDbSaveMode(SaveMode.Overwrite)))
  }

  ignore("save db table to disk") {
    table.in(db).save()
  }

  test("read table on disk") {
    val rst = table.in(db).fromDisk.collect[IO].map(_.head).unsafeRunSync
    assert(rst === sample)
  }
}
