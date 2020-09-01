package mtest.spark.database

import java.sql.Date
import java.time._

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.common.transformers._
import com.github.chenharryhua.nanjin.database.TableName
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.database._
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.persist.loaders
import frameless.TypedDataset
import frameless.cats.implicits._
import io.circe.generic.auto._
import io.scalaland.chimney.dsl._
import kantan.csv.generic._
import kantan.csv.java8._
import org.scalatest.funsuite.AnyFunSuite
import cats.derived.auto.show._
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec

final case class DomainObject(
  a: LocalDate,
  b: Date,
  c: ZonedDateTime,
  d: OffsetDateTime,
  e: Instant)

final case class DBTable(a: LocalDate, b: LocalDate, c: Instant, d: Instant, e: Instant)

class SparkTableTest extends AnyFunSuite {
  implicit val zoneId: ZoneId = beijingTime

  implicit val codec                          = NJAvroCodec[DBTable]
  implicit val ate: AvroTypedEncoder[DBTable] = AvroTypedEncoder[DBTable](codec)

  val table: TableDef[DBTable] = TableDef[DBTable](TableName("public.sparktabletest"))

  val sample: DomainObject =
    DomainObject(
      LocalDate.now,
      Date.valueOf(LocalDate.now),
      ZonedDateTime.now(zoneId),
      OffsetDateTime.now(zoneId),
      Instant.now)

  val dbData: DBTable = sample.transformInto[DBTable]

  test("sparkTable upload dataset to table") {
    val data = TypedDataset.create(List(sample.transformInto[DBTable])).dataset.rdd
    sparkSession
      .alongWith[IO](postgres)
      .table(table)
      .tableDataset(data)
      .upload
      .overwrite
      .run
      .unsafeRunSync()
  }

  test("partition save") {
    /*  val run = table.in[IO](postgres).fromDB.save.partition.jackson.run(blocker) >>
      table.in[IO](postgres).fromDB.save.partition.avro.run(blocker) >>
      table.in[IO](postgres).fromDB.save.partition.parquet.run(blocker) >>
      table.in[IO](postgres).fromDB.save.partition.circe.run(blocker) >>
      table.in[IO](postgres).fromDB.save.partition.csv.run(blocker) >>
      IO(())
    run.unsafeRunSync
     */
  }
  test("save and load") {
    val root  = "./data/test/spark/database/postgres/"
    val saver = table.in[IO](postgres).fromDB.save
    val run =
      saver.avro(root + "multi.spark.avro").multi.spark.run(blocker) >>
        saver.avro(root + "single.raw.avro").single.raw.run(blocker) >>
        saver.avro(root + "raw.avro").raw.run(blocker) >>
        saver.parquet(root + "multi.spark.parquet").multi.spark.run(blocker) >>
        saver.parquet(root + "single.raw.parquet").single.raw.run(blocker) >>
        saver.parquet(root + "raw.parquet").raw.run(blocker) >>
        saver.circe(root + "multi.circe.json").multi.run(blocker) >>
        saver.circe(root + "single.circe.json").single.run(blocker) >>
        saver.text(root + "multi.text").multi.run(blocker) >>
        saver.text(root + "single.text").single.run(blocker) >>
        saver.csv(root + "multi.csv").multi.run(blocker) >>
        saver.csv(root + "single.csv").single.run(blocker)

    run.unsafeRunSync()

    assert(loaders.avro(root + "multi.spark.avro").collect[IO]().unsafeRunSync().head == dbData)
    assert(loaders.raw.avro(root + "single.raw.avro").collect().head == dbData)
    assert(loaders.raw.avro(root + "raw.avro").collect.head == dbData)

    assert(
      loaders.parquet(root + "multi.spark.parquet").collect[IO]().unsafeRunSync().head == dbData)
    assert(loaders.raw.parquet(root + "single.raw.parquet").collect.head == dbData)
    assert(loaders.raw.parquet(root + "raw.parquet").collect.head == dbData)

    assert(loaders.circe[DBTable](root + "multi.circe.json").collect().head == dbData)
    assert(loaders.circe[DBTable](root + "single.circe.json").collect().head == dbData)

  }
  test("with query") {}
}
