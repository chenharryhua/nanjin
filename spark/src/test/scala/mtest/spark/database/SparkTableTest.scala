package mtest.spark.database

import java.sql.Date
import java.time._

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.common.transformers._
import com.github.chenharryhua.nanjin.database.TableName
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.database._
import com.github.chenharryhua.nanjin.spark.injection._
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

class SparkTableTest extends AnyFunSuite {
  implicit val zoneId: ZoneId = beijingTime

  implicit val codec: NJAvroCodec[DBTable]    = NJAvroCodec[DBTable]
  implicit val te: TypedEncoder[DBTable]      = shapeless.cachedImplicit
  implicit val ate: AvroTypedEncoder[DBTable] = AvroTypedEncoder[DBTable](codec)
  implicit val re: RowEncoder[DBTable]        = shapeless.cachedImplicit

  val table: TableDef[DBTable] = TableDef[DBTable](TableName("sparktest"), ate)

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
    val run = table.in[IO](postgres).fromDB.partition.jackson.run(blocker) >>
      table.in[IO](postgres).fromDB.partition.avro.run(blocker) >>
      table.in[IO](postgres).fromDB.partition.parquet.run(blocker) >>
      table.in[IO](postgres).fromDB.partition.circe.run(blocker) >>
      table.in[IO](postgres).fromDB.partition.csv.run(blocker) >>
      IO(())
    run.unsafeRunSync
  }
  val root                        = "./data/test/spark/database/postgres/"
  val tb: SparkTable[IO, DBTable] = table.in[IO](postgres)

  val loader = tb.load
  val saver  = tb.fromDB.save

  test("avro") {

    val avro = saver.avro(root + "multi.spark.avro").folder.spark.run(blocker) >>
      saver.avro(root + "single.raw.avro").file.raw.run(blocker) >>
      saver.avro(root + "multi.raw.avro").folder.raw.run(blocker)
    avro.unsafeRunSync()
    assert(loader.avro(root + "multi.spark.avro").dataset.collect.head == dbData)
    assert(loader.avro(root + "single.raw.avro").dataset.collect.head == dbData)
    assert(loader.avro(root + "multi.raw.avro").dataset.collect.head == dbData)

  }
  test("parquet") {
    val parquet = saver.parquet(root + "multi.spark.parquet").folder.spark.run(blocker) >>
      saver.parquet(root + "single.raw.parquet").file.raw.run(blocker) >>
      saver.parquet(root + "raw.parquet").raw.run(blocker)
    parquet.unsafeRunSync()
    assert(loader.parquet(root + "multi.spark.parquet").dataset.collect.head == dbData)
    assert(loader.parquet(root + "single.raw.parquet").dataset.collect.head == dbData)
    assert(loader.parquet(root + "raw.parquet").dataset.collect.head == dbData)

  }
  test("circe") {
    val circe = saver.circe(root + "multi.circe.json").folder.run(blocker) >>
      saver.circe(root + "single.circe.json").file.run(blocker)
    circe.unsafeRunSync()
    assert(loader.circe(root + "multi.circe.json").dataset.collect.head == dbData)
    assert(loader.circe(root + "single.circe.json").dataset.collect.head == dbData)
  }

  test("csv") {
    val csv = saver.csv(root + "multi.csv").folder.run(blocker) >>
      saver.csv(root + "single.csv").file.run(blocker)
    csv.unsafeRunSync()
    assert(loader.csv(root + "multi.csv").dataset.collect.head == dbData)
    assert(loader.csv(root + "single.csv").dataset.collect.head == dbData)
  }
  test("spark json") {
    val json = saver.json(root + "spark.json").run(blocker)
    json.unsafeRunSync()
    assert(loader.json(root + "spark.json").dataset.collect.head == dbData)
  }
}
