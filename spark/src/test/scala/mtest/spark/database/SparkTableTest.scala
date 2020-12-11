package mtest.spark.database

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.common.transformers._
import com.github.chenharryhua.nanjin.database.TableName
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.database._
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.persist.DatasetAvroFileHoarder
import doobie.implicits._
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import io.circe.generic.auto._
import io.scalaland.chimney.dsl._
import kantan.csv.generic._
import kantan.csv.java8._
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Date
import java.time._

final case class DomainObject(
  a: LocalDate,
  b: Date,
  c: ZonedDateTime,
  d: OffsetDateTime,
  e: Instant)

final case class DBTable(a: LocalDate, b: LocalDate, c: Instant, d: Instant, e: Instant)
final case class PartialDBTable(a: LocalDate, b: LocalDate)

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

  val codec: AvroCodec[DBTable]                  = AvroCodec[DBTable]
  implicit val te: TypedEncoder[DBTable]         = shapeless.cachedImplicit
  implicit val te2: TypedEncoder[PartialDBTable] = shapeless.cachedImplicit
  implicit val re: RowEncoder[DBTable]           = shapeless.cachedImplicit

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
    val data = TypedDataset.create(List(sample.transformInto[DBTable]))
    table.in[IO](postgres).tableset(data).upload.overwrite.run.unsafeRunSync()

  }

  test("dump and count") {
    table.in[IO](postgres).dump.unsafeRunSync()
    val dbc = table.in[IO](postgres).countDB
    val dc  = table.in[IO](postgres).countDisk

    val load = table.in[IO](postgres).fromDB.dataset
    val l1   = table.in[IO](postgres).tableset(load).typedDataset
    val l2   = table.in[IO](postgres).tableset(load.rdd).typedDataset

    assert(dbc == dc)
    assert(l1.except(l2).count[IO]().unsafeRunSync() == 0)
  }

  test("partial db table") {
    val pt: TableDef[PartialDBTable] = TableDef[PartialDBTable](TableName("sparktest"))
    val ptd: TypedDataset[PartialDBTable] = pt
      .in[IO](postgres)
      .withQuery("select a,b from sparktest")
      .withReplayPathBuilder((_, _) => root + "dbdump")
      .fromDB
      .repartition(1)
      .typedDataset
    val pt2: TableDef[PartialDBTable] =
      TableDef[PartialDBTable](
        TableName("sparktest"),
        AvroCodec[PartialDBTable],
        "select a,b from sparktest")

    val ptd2: TypedDataset[PartialDBTable] =
      pt2.in[IO](postgres).fromDB.typedDataset

    val pate = AvroTypedEncoder[PartialDBTable]
    val ptd3: TypedDataset[PartialDBTable] = table
      .in[IO](postgres)
      .fromDB
      .map(_.transformInto[PartialDBTable])(pate)
      .flatMap(Option(_))(pate)
      .typedDataset

    assert(ptd.except(ptd2).count[IO]().unsafeRunSync() == 0)
    assert(ptd.except(ptd3).count[IO]().unsafeRunSync() == 0)

  }

  val root = "./data/test/spark/database/postgres/"

  val tb: SparkTable[IO, DBTable] = table.in[IO](postgres)

  val saver: DatasetAvroFileHoarder[IO, DBTable] = tb.fromDB.save

  test("save avro") {
    val avro = saver.avro(root + "single.raw.avro").file.run(blocker) >>
      saver.avro(root + "multi.raw.avro").folder.run(blocker)
    avro.unsafeRunSync()
    assert(table.load.avro(root + "single.raw.avro").dataset.collect.head == dbData)
    assert(table.load.avro(root + "multi.raw.avro").dataset.collect.head == dbData)
  }

  test("save bin avro") {
    val avro = saver.binAvro(root + "single.binary.avro").file.run(blocker)
    avro.unsafeRunSync()
    assert(table.load.binAvro(root + "single.binary.avro").dataset.collect.head == dbData)
  }

  test("save jackson") {
    val avro = saver.jackson(root + "single.jackson.json").file.run(blocker)
    avro.unsafeRunSync()
    assert(table.load.jackson(root + "single.jackson.json").dataset.collect.head == dbData)
  }

  test("save parquet") {
    val parquet = saver.parquet(root + "multi.parquet").run(blocker)
    parquet.unsafeRunSync()
    assert(table.load.parquet(root + "multi.parquet").dataset.collect.head == dbData)
  }
  test("save circe") {
    val circe = saver.circe(root + "multi.circe.json").folder.run(blocker) >>
      saver.circe(root + "single.circe.json").file.run(blocker)
    circe.unsafeRunSync()
    assert(table.load.circe(root + "multi.circe.json").dataset.collect.head == dbData)
    assert(table.load.circe(root + "single.circe.json").dataset.collect.head == dbData)
  }

  test("save csv") {
    val csv = saver.csv(root + "multi.csv").folder.run(blocker) >>
      saver.csv(root + "single.csv").file.run(blocker)
    csv.unsafeRunSync()
    assert(table.load.csv(root + "multi.csv").dataset.collect.head == dbData)
    assert(table.load.csv(root + "single.csv", CsvConfiguration.rfc).dataset.collect.head == dbData)
  }
  test("save spark json") {
    val json = saver.json(root + "spark.json").run(blocker)
    json.unsafeRunSync()
    assert(table.load.json(root + "spark.json").dataset.collect.head == dbData)
  }
}
