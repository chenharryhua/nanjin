package mtest.spark.database

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.transformers.*
import com.github.chenharryhua.nanjin.database.NJHikari
import com.github.chenharryhua.nanjin.datetime.sydneyTime
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, SparkSessionExt}
import com.github.chenharryhua.nanjin.spark.injection.*
import com.github.chenharryhua.nanjin.terminals.NJPath
import doobie.ExecutionContexts
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import eu.timepit.refined.auto.*
import frameless.{TypedDataset, TypedEncoder}
import io.scalaland.chimney.dsl.*
import kantan.csv.{RowDecoder, RowEncoder}
import kantan.csv.generic.*
import kantan.csv.java8.*
import mtest.spark.sparkSession
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import io.circe.generic.auto.*

import java.sql.Date
import java.time.*
import java.time.temporal.ChronoUnit
import scala.util.Random

final case class DomainObject(
  a: LocalDate,
  b: Date,
  c: ZonedDateTime,
  d: OffsetDateTime,
  e: Option[Instant]) {
  def toDB: DBTable = this
    .into[DBTable]
    .withFieldComputed(_.b, _.b.toLocalDate)
    .withFieldComputed(_.c, _.c.toInstant)
    .withFieldComputed(_.d, _.d.toInstant)
    .transform
}

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

  implicit val ss: SparkSession = sparkSession

  val codec: NJAvroCodec[DBTable]                = NJAvroCodec[DBTable]
  implicit val te: TypedEncoder[DBTable]         = shapeless.cachedImplicit
  implicit val te2: TypedEncoder[PartialDBTable] = shapeless.cachedImplicit
  implicit val re: RowEncoder[DBTable]           = shapeless.cachedImplicit
  implicit val rd: RowDecoder[DBTable]           = shapeless.cachedImplicit
  val ate: AvroTypedEncoder[DBTable]             = AvroTypedEncoder[DBTable](te, codec)

  val sample: DomainObject =
    DomainObject(
      LocalDate.now,
      Date.valueOf(LocalDate.now),
      ZonedDateTime.now(sydneyTime).truncatedTo(ChronoUnit.MILLIS),
      OffsetDateTime.now(sydneyTime).truncatedTo(ChronoUnit.MILLIS),
      if (Random.nextBoolean()) Some(Instant.now.truncatedTo(ChronoUnit.MILLIS)) else None
    )

  val dbData: DBTable = sample.toDB

  val pg: Resource[IO, HikariTransactor[IO]] =
    NJHikari(postgres).transactorResource(ExecutionContexts.fixedThreadPool[IO](10))

  pg.use(txn => (DBTable.drop *> DBTable.create).transact(txn)).unsafeRunSync()

  val hikari = NJHikari(postgres).hikariConfig
  val loader = ss.loadWith(ate)

  test("load data") {
    val tds = TypedDataset.create(List(dbData))
    val d1  = loader.data(List(dbData))
    val d2  = loader.data(tds)
    val d3  = loader.data(tds.dataset)
    val d4  = loader.data(tds.dataset.rdd)
    assert(d1.diff(d2).dataset.count() == 0)
    assert(d3.diff(d4).dataset.count() == 0)
  }

  test("upload dataset to table") {
    val data = TypedDataset.create(List(dbData))
    loader.data(data).upload[IO](hikari, "sparktest", SaveMode.Overwrite).unsafeRunSync()
  }

  test("dump and count") {
    val d = loader.jdbc(hikari, "sparktest").dataset.collect().head
    assert(d == dbData)
  }

  test("save/load") {
    loader.jdbc(hikari, "sparktest").save[IO].avro(root / "avro").run.unsafeRunSync()
    loader.avro(root / "avro").save[IO].binAvro(root / "bin.avro").run.unsafeRunSync()
    loader.avro(root / "avro").save[IO].jackson(root / "jackson").run.unsafeRunSync()
    loader.avro(root / "avro").save[IO].circe(root / "circe").run.unsafeRunSync()
    loader.avro(root / "avro").save[IO].kantan(root / "kantan").run.unsafeRunSync()
    loader.avro(root / "avro").save[IO].parquet(root / "parquet").run.unsafeRunSync()
    loader.avro(root / "avro").save[IO].objectFile(root / "obj").run.unsafeRunSync()

    val avro    = loader.avro(root / "avro")
    val binAvro = loader.binAvro(root / "bin.avro")
    val jackson = loader.jackson(root / "jackson")
    val circe   = loader.circe(root / "circe")
    val kantan  = loader.kantan(root / "kantan")
    val parquet = loader.parquet(root / "parquet")
    val obj     = loader.objectFile(root / "obj")
    assert(avro.diff(binAvro).dataset.count() == 0)
    assert(jackson.diff(circe).dataset.count() == 0)
    assert(kantan.diff(parquet).dataset.count() == 0)
    assert(kantan.diff(obj).dataset.count() == 0)
  }

  test("spark") {
    val parquet = root / "spark" / "parquet"
    val json    = root / "spark" / "json"
    val csv     = root / "spark" / "csv"
    val avro    = root / "spark" / "avro"

    loader.avro(root / "avro").dataset.write.mode(SaveMode.Overwrite).parquet(parquet.pathStr)
    loader.avro(root / "avro").dataset.write.mode(SaveMode.Overwrite).json(json.pathStr)
    loader.avro(root / "avro").dataset.write.mode(SaveMode.Overwrite).csv(csv.pathStr)
    loader.avro(root / "avro").dataset.write.mode(SaveMode.Overwrite).format("avro").save(avro.pathStr)

    assert(loader.spark.csv(csv).diff(loader.spark.json(json)).dataset.count() == 0)
    assert(loader.spark.avro(avro).diff(loader.spark.parquet(parquet)).dataset.count() == 0)
  }

}
