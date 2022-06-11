package mtest.spark.database

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.database.TableName
import com.github.chenharryhua.nanjin.common.transformers.*
import com.github.chenharryhua.nanjin.database.NJHikari
import com.github.chenharryhua.nanjin.datetime.*
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.database.*
import com.github.chenharryhua.nanjin.spark.injection.*
import com.github.chenharryhua.nanjin.spark.persist.DatasetAvroFileHoarder
import com.github.chenharryhua.nanjin.terminals.NJPath
import doobie.ExecutionContexts
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import eu.timepit.refined.auto.*
import frameless.{TypedDataset, TypedEncoder}
import io.circe.generic.auto.*
import io.scalaland.chimney.dsl.*
import kantan.csv.RowEncoder
import kantan.csv.generic.*
import kantan.csv.java8.*
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
  val root = NJPath("./data/test/spark/database/postgres/")

  implicit val zoneId: ZoneId = beijingTime

  implicit val ss: SparkSession = sparkSession

  val codec: NJAvroCodec[DBTable]                = NJAvroCodec[DBTable]
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

  val pg: Resource[IO, HikariTransactor[IO]] =
    NJHikari(postgres).transactorResource(ExecutionContexts.fixedThreadPool[IO](10))

  pg.use(txn => (DBTable.drop *> DBTable.create).transact(txn)).unsafeRunSync()

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
        .withReplayPathBuilder(_ => root / "dbdump")
        .fromDB
        .map(_.repartition(1).typedDataset)
        .unsafeRunSync()
    val pt2: TableDef[PartialDBTable] =
      TableDef[PartialDBTable](
        TableName("sparktest"),
        NJAvroCodec[PartialDBTable],
        "select a,b from sparktest")

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

  val tb: SparkDBTable[IO, DBTable] = sparkDB.table(table)

  def saver: DatasetAvroFileHoarder[IO, DBTable] = tb.fromDB.map(_.save).unsafeRunSync()

  test("save avro") {
    val avro = saver.avro(root / "multi.raw.avro").run
    avro.unsafeRunSync()
    val mhead = tb.load.avro(root / "multi.raw.avro").map(_.dataset.collect().head).unsafeRunSync()
    assert(mhead == dbData)
  }

  test("save bin avro") {
    val avro = saver.binAvro(root / "binary.avro").run
    avro.unsafeRunSync()
    val head = tb.load.binAvro(root / "binary.avro").map(_.dataset.collect().head)
    assert(head.unsafeRunSync() == dbData)
  }

  test("save parquet") {
    val parquet = saver.parquet(root / "multi.parquet").run
    parquet.unsafeRunSync()
    val head = tb.load.parquet(root / "multi.parquet").map(_.dataset.collect().head)
    assert(head.unsafeRunSync() == dbData)
  }
  test("save circe") {
    val circe = saver.circe(root / "multi.circe.json").run
    circe.unsafeRunSync()
    val mhead = tb.load.circe(root / "multi.circe.json").map(_.dataset.collect().head)
    assert(mhead.unsafeRunSync() == dbData)
  }

  test("save csv") {
    val csv = saver.csv(root / "multi.csv").run
    csv.unsafeRunSync()
    val mhead = tb.load.csv(root / "multi.csv").map(_.dataset.collect().head)
    assert(mhead.unsafeRunSync() == dbData)
  }
  test("save spark json") {
    val json = saver.json(root / "spark.json").run
    json.unsafeRunSync()
    val head = tb.load.json(root / "spark.json").map(_.dataset.collect().head)
    assert(head.unsafeRunSync() == dbData)
  }
}
