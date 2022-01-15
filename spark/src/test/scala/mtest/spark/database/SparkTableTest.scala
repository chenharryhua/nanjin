package mtest.spark.database

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.database.{Postgres, TableName}
import com.github.chenharryhua.nanjin.common.transformers.*
import com.github.chenharryhua.nanjin.database.NJHikari
import com.github.chenharryhua.nanjin.datetime.*
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.database.*
import com.github.chenharryhua.nanjin.spark.injection.*
import com.github.chenharryhua.nanjin.spark.persist.DatasetAvroFileHoarder
import doobie.implicits.*
import frameless.{TypedDataset, TypedEncoder}
import io.circe.generic.auto.*
import io.scalaland.chimney.dsl.*
import kantan.csv.generic.*
import kantan.csv.java8.*
import kantan.csv.{CsvConfiguration, RowEncoder}
import mtest.spark.sparkSession
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Date
import java.time.*
import java.time.temporal.ChronoUnit
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
      ZonedDateTime.now(zoneId).truncatedTo(ChronoUnit.MILLIS),
      OffsetDateTime.now(zoneId).truncatedTo(ChronoUnit.MILLIS),
      if (Random.nextBoolean()) Some(Instant.now.truncatedTo(ChronoUnit.MILLIS)) else None
    )

  val dbData: DBTable = sample.transformInto[DBTable]

  NJHikari[Postgres].runQuery[IO, Int](postgres)(DBTable.drop *> DBTable.create).unsafeRunSync()

  test("sparkTable upload dataset to table") {
    val data = TypedDataset.create(List(sample.transformInto[DBTable]))
    sparkDB.table(table).tableset(data).upload.overwrite.run.unsafeRunSync()

  }

  test("dump and count") {
    sparkDB.table(table).dump.unsafeRunSync()
    val dbc = sparkDB.table(table).countDB.unsafeRunSync()
    val dc  = sparkDB.table(table).countDisk.unsafeRunSync()

    val load = sparkDB.table(table).fromDB.map(_.dataset).unsafeRunSync()
    val l1   = sparkDB.table(table).tableset(load)
    val l2   = sparkDB.table(table).tableset(load.rdd).typedDataset

    assert(dbc == dc)
    assert(l1.typedDataset.except(l2).dataset.count() == 0)
  }

  test("partial db table") {
    val pt = sparkDB.table[PartialDBTable]("sparktest")
    val ptd: TypedDataset[PartialDBTable] =
      pt.withQuery("select a,b from sparktest")
        .withReplayPathBuilder(_ => root + "dbdump")
        .fromDB
        .map(_.repartition(1).typedDataset)
        .unsafeRunSync()
    val pt2: TableDef[PartialDBTable] =
      TableDef[PartialDBTable](TableName("sparktest"), AvroCodec[PartialDBTable], "select a,b from sparktest")

    val ptd2: TypedDataset[PartialDBTable] = sparkDB.table(pt2).fromDB.map(_.typedDataset).unsafeRunSync()

    val ptd3: TypedDataset[PartialDBTable] =
      sparkDB
        .table(table)
        .fromDB
        .map(_.map(_.transformInto[PartialDBTable])(pt.tableDef).flatMap(Option(_))(pt.tableDef).typedDataset)
        .unsafeRunSync()

    assert(ptd.except(ptd2).dataset.count() == 0)
    assert(ptd.except(ptd3).dataset.count() == 0)

  }

  val root = "./data/test/spark/database/postgres/"

  val tb: SparkDBTable[IO, DBTable] = sparkDB.table(table)

  val saver: DatasetAvroFileHoarder[IO, DBTable] = tb.fromDB.map(_.save).unsafeRunSync()

  test("save avro") {
    val avro = saver.avro(root + "single.raw.avro").file.sink.compile.drain >>
      saver.avro(root + "multi.raw.avro").folder.run
    avro.unsafeRunSync()
    val shead = tb.load.avro(root + "single.raw.avro").map(_.dataset.collect().head).unsafeRunSync()
    assert(shead == dbData)
    val mhead = tb.load.avro(root + "multi.raw.avro").map(_.dataset.collect().head).unsafeRunSync()
    assert(mhead == dbData)
  }

  test("save bin avro") {
    val avro = saver.binAvro(root + "single.binary.avro").file.sink.compile.drain
    avro.unsafeRunSync()
    val head = tb.load.binAvro(root + "single.binary.avro").map(_.dataset.collect().head)
    assert(head.unsafeRunSync() == dbData)
  }

  test("save jackson") {
    val avro = saver.jackson(root + "single.jackson.json").file.sink.compile.drain
    avro.unsafeRunSync()
    val head = tb.load.jackson(root + "single.jackson.json").map(_.dataset.collect().head)
    assert(head.unsafeRunSync() == dbData)
  }

  test("save parquet") {
    val parquet = saver.parquet(root + "multi.parquet").folder.run
    parquet.unsafeRunSync()
    val head = tb.load.parquet(root + "multi.parquet").map(_.dataset.collect().head)
    assert(head.unsafeRunSync() == dbData)
  }
  test("save circe") {
    val circe = saver.circe(root + "multi.circe.json").folder.run >>
      saver.circe(root + "single.circe.json").file.sink.compile.drain
    circe.unsafeRunSync()
    val mhead = tb.load.circe(root + "multi.circe.json").map(_.dataset.collect().head)
    assert(mhead.unsafeRunSync() == dbData)
    val shead = tb.load.circe(root + "single.circe.json").map(_.dataset.collect().head)
    assert(shead.unsafeRunSync() == dbData)
  }

  test("save csv") {
    val csv = saver.csv(root + "multi.csv").folder.run >>
      saver.csv(root + "single.csv").file.sink.compile.drain
    csv.unsafeRunSync()
    val mhead = tb.load.csv(root + "multi.csv").map(_.dataset.collect().head)
    assert(mhead.unsafeRunSync() == dbData)
    val shead = tb.load.csv(root + "single.csv", CsvConfiguration.rfc).map(_.dataset.collect().head)
    assert(shead.unsafeRunSync() == dbData)
  }
  test("save spark json") {
    val json = saver.json(root + "spark.json").run
    json.unsafeRunSync()
    val head = tb.load.json(root + "spark.json").map(_.dataset.collect().head)
    assert(head.unsafeRunSync() == dbData)
  }
}
