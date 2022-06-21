package mtest.spark.table

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
import com.github.chenharryhua.nanjin.spark.table.LoadTable
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.zaxxer.hikari.HikariConfig
import doobie.ExecutionContexts
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import eu.timepit.refined.auto.*
import frameless.{TypedDataset, TypedEncoder}
import io.scalaland.chimney.dsl.*
import kantan.csv.{CsvConfiguration, RowDecoder, RowEncoder}
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

  val hikari: HikariConfig           = NJHikari(postgres).hikariConfig
  val loader: LoadTable[IO, DBTable] = ss.loadTable[IO](ate)

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
    loader.data(data).upload(hikari, "sparktest", SaveMode.Overwrite).unsafeRunSync()
  }

  test("dump and count") {
    val d = loader.jdbc(hikari, "sparktest").dataset.collect().head
    assert(d == dbData)
  }

  test("save/load") {
    loader.jdbc(hikari, "sparktest").save.avro(root / "base").run.unsafeRunSync()

    loader.avro(root / "base").save.avro(root / "avro" / 1).run.unsafeRunSync()
    loader.avro(root / "base").save.binAvro(root / "bin.avro" / 1).run.unsafeRunSync()
    loader.avro(root / "base").save.jackson(root / "jackson" / 1).run.unsafeRunSync()
    loader.avro(root / "base").save.circe(root / "circe" / 1).run.unsafeRunSync()
    loader.avro(root / "base").save.kantan(root / "kantan" / 1).run.unsafeRunSync()
    loader.avro(root / "base").save.parquet(root / "parquet" / 1).run.unsafeRunSync()
    loader.avro(root / "base").save.objectFile(root / "obj" / 1).run.unsafeRunSync()

    val avro    = loader.avro(root / "avro" / 1)
    val binAvro = loader.binAvro(root / "bin.avro" / 1)
    val jackson = loader.jackson(root / "jackson" / 1)
    val circe   = loader.circe(root / "circe" / 1)
    val kantan  = loader.kantan(root / "kantan" / 1, CsvConfiguration.rfc)
    val parquet = loader.parquet(root / "parquet" / 1)
    val obj     = loader.objectFile(root / "obj" / 1)

    assert(avro.diff(binAvro).dataset.count() == 0)
    assert(jackson.diff(circe).dataset.count() == 0)
    assert(kantan.diff(parquet).dataset.count() == 0)
    assert(kantan.diff(obj).dataset.count() == 0)
  }

  test("save/load all") {
    loader.avro(root / "base").save.avro(root / "avro" / 2).run.unsafeRunSync()
    loader.avro(root / "base").save.binAvro(root / "bin.avro" / 2).run.unsafeRunSync()
    loader.avro(root / "base").save.jackson(root / "jackson" / 2).run.unsafeRunSync()
    loader.avro(root / "base").save.circe(root / "circe" / 2).run.unsafeRunSync()
    loader.avro(root / "base").save.kantan(root / "kantan" / 2).run.unsafeRunSync()
    loader.avro(root / "base").save.parquet(root / "parquet" / 2).run.unsafeRunSync()
    loader.avro(root / "base").save.objectFile(root / "obj" / 2).run.unsafeRunSync()

    //  val obj = loader.objectFileAll(root / "obj")

    val avro_bin = for {
      a <- loader.avroAll(root / "avro")
      b <- loader.binAvroAll(root / "bin.avro")
    } yield a.diff(b).dataset.count()

    val jackson_circe = for {
      a <- loader.jacksonAll(root / "jackson")
      b <- loader.circeAll(root / "circe")
    } yield a.diff(b).dataset.count()

    val kantan_parquet = for {
      a <- loader.kantanAll(root / "kantan", CsvConfiguration.rfc)
      b <- loader.parquetAll(root / "parquet")
    } yield a.diff(b).dataset.count()

    val avro_obj = for {
      a <- loader.avroAll(root / "avro")
      b <- loader.objectFileAll(root / "obj")
    } yield a.diff(b).dataset.count()

    assert(avro_bin.unsafeRunSync() == 0)
    assert(jackson_circe.unsafeRunSync() == 0)
    assert(kantan_parquet.unsafeRunSync() == 0)
    assert(avro_obj.unsafeRunSync() == 0)
  }

  test("spark") {
    val parquet = root / "spark" / "parquet"
    val json    = root / "spark" / "json"
    val csv     = root / "spark" / "csv"
    val avro    = root / "spark" / "avro"

    loader.avro(root / "base").dataset.write.mode(SaveMode.Overwrite).parquet(parquet.pathStr)
    loader.avro(root / "base").dataset.write.mode(SaveMode.Overwrite).json(json.pathStr)
    loader.avro(root / "base").dataset.write.mode(SaveMode.Overwrite).csv(csv.pathStr)
    loader.avro(root / "base").dataset.write.mode(SaveMode.Overwrite).format("avro").save(avro.pathStr)

    assert(loader.spark.csv(csv).diff(loader.spark.json(json)).dataset.count() == 0)
    assert(loader.spark.avro(avro).diff(loader.spark.parquet(parquet)).dataset.count() == 0)
  }

}
