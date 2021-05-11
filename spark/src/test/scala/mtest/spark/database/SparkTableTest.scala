package mtest.spark.database

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import com.github.chenharryhua.nanjin.common.transformers._
import com.github.chenharryhua.nanjin.database.TableName
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.database._
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.persist.DatasetAvroFileHoarder
import doobie.implicits._
import frameless.{TypedDataset, TypedEncoder}
import io.circe.generic.auto._
import io.scalaland.chimney.dsl._
import kantan.csv.generic._
import kantan.csv.java8._
import kantan.csv.{CsvConfiguration, RowEncoder}
import mtest.spark.sparkSession
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Date
import java.time._
import scala.util.Random
final case class DomainObject(a: LocalDate, b: Date, c: ZonedDateTime, d: OffsetDateTime, e: Option[Instant])

final case class DBTable(a: LocalDate, b: LocalDate, c: Instant, d: Instant, e: Option[Instant])
final case class PartialDBTable(a: LocalDate, b: LocalDate)

object DBTable {

  val drop: doobie.ConnectionIO[Int] =
    sql"DROP TABLE IF EXISTS sparktest".update.run

  val create: doobie.ConnectionIO[Int] =
    sql"""
         CREATE TABLE sparktest (
             a  date NOT NULL,
             b  date NOT NULL,
             c  timestamp NOT NULL,
             d  timestamp NOT NULL,
             e  timestamp
         );""".update.run
}

class SparkTableTest extends AnyFunSuite {
  implicit val zoneId: ZoneId = beijingTime

  implicit val ss: SparkSession = sparkSession

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
      if (Random.nextBoolean()) Some(Instant.now) else None)

  val dbData: DBTable = sample.transformInto[DBTable]

  postgres.runQuery[IO, Int](DBTable.drop *> DBTable.create).unsafeRunSync()

  test("sparkTable upload dataset to table") {
    val data = TypedDataset.create(List(sample.transformInto[DBTable]))
    sparkDB.table(table).tableset(data).upload.overwrite.run.unsafeRunSync()

  }

  test("dump and count") {
    sparkDB.table(table).dump.unsafeRunSync()
    val dbc = sparkDB.table(table).countDB
    val dc  = sparkDB.table(table).countDisk

    val load = sparkDB.table(table).fromDB.dataset
    val l1   = sparkDB.table(table).tableset(load)
    val l2   = sparkDB.table(table).tableset(load.rdd).typedDataset

    assert(dbc == dc)
    assert(l1.typedDataset.except(l2).dataset.count() == 0)
  }

  test("partial db table") {
    val pt = sparkDB.table[PartialDBTable]("sparktest")
    val ptd: TypedDataset[PartialDBTable] =
      pt.withQuery("select a,b from sparktest")
        .withReplayPathBuilder((_, _) => root + "dbdump")
        .fromDB
        .repartition(1)
        .typedDataset
    val pt2: TableDef[PartialDBTable] =
      TableDef[PartialDBTable](TableName("sparktest"), AvroCodec[PartialDBTable], "select a,b from sparktest")

    val ptd2: TypedDataset[PartialDBTable] = sparkDB.table(pt2).fromDB.typedDataset

    val ptd3: TypedDataset[PartialDBTable] =
      sparkDB
        .table(table)
        .fromDB
        .map(_.transformInto[PartialDBTable])(pt.tableDef)
        .flatMap(Option(_))(pt.tableDef)
        .typedDataset

    assert(ptd.except(ptd2).dataset.count() == 0)
    assert(ptd.except(ptd3).dataset.count() == 0)

  }

  val root = "./data/test/spark/database/postgres/"

  val tb: SparkDBTable[IO, DBTable] = sparkDB.table(table)

  val saver: DatasetAvroFileHoarder[IO, DBTable] = tb.fromDB.save

  test("save avro") {
    val avro = saver.avro(root + "single.raw.avro").file.run >>
      saver.avro(root + "multi.raw.avro").folder.run
    avro.unsafeRunSync()
    assert(tb.load.avro(root + "single.raw.avro").dataset.collect.head == dbData)
    assert(tb.load.avro(root + "multi.raw.avro").dataset.collect.head == dbData)
  }

  test("save bin avro") {
    val avro = saver.binAvro(root + "single.binary.avro").file.run
    avro.unsafeRunSync()
    assert(tb.load.binAvro(root + "single.binary.avro").dataset.collect.head == dbData)
  }

  test("save jackson") {
    val avro = saver.jackson(root + "single.jackson.json").file.run
    avro.unsafeRunSync()
    assert(tb.load.jackson(root + "single.jackson.json").dataset.collect.head == dbData)
  }

  test("save parquet") {
    val parquet = saver.parquet(root + "multi.parquet").folder.run
    parquet.unsafeRunSync()
    assert(tb.load.parquet(root + "multi.parquet").dataset.collect.head == dbData)
  }
  test("save circe") {
    val circe = saver.circe(root + "multi.circe.json").folder.run >>
      saver.circe(root + "single.circe.json").file.run
    circe.unsafeRunSync()
    assert(tb.load.circe(root + "multi.circe.json").dataset.collect.head == dbData)
    assert(tb.load.circe(root + "single.circe.json").dataset.collect.head == dbData)
  }

  test("save csv") {
    val csv = saver.csv(root + "multi.csv").folder.run >>
      saver.csv(root + "single.csv").file.run
    csv.unsafeRunSync()
    assert(tb.load.csv(root + "multi.csv").dataset.collect.head == dbData)
    assert(tb.load.csv(root + "single.csv", CsvConfiguration.rfc).dataset.collect.head == dbData)
  }
  test("save spark json") {
    val json = saver.json(root + "spark.json").run
    json.unsafeRunSync()
    assert(tb.load.json(root + "spark.json").dataset.collect.head == dbData)
  }
  test("show schemas - spark does not respect not null") {
    println("--- spark ---")
    println(sparkDB.genCaseClass("sparktest"))
    println(sparkDB.genSchema("sparktest"))
    println(sparkDB.genDatatype("sparktest"))
    println("--- db ---")
    println(postgres.genCaseClass[IO].unsafeRunSync())
  }
}
