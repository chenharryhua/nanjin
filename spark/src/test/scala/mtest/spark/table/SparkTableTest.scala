package mtest.spark.table

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.database.DBConfig
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.listeners.SparkContextListener
import com.github.chenharryhua.nanjin.spark.table.LoadTable
import com.github.chenharryhua.nanjin.spark.{SchematizedEncoder, SparkSessionExt}
import io.lemonlabs.uri.typesafe.dsl.*
import com.zaxxer.hikari.HikariConfig
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import eu.timepit.refined.auto.*
import frameless.{TypedDataset, TypedEncoder}
import io.circe.generic.auto.*
import io.scalaland.chimney.dsl.*
import kantan.csv.generic.*
import kantan.csv.java8.*
import kantan.csv.{CsvConfiguration, RowDecoder, RowEncoder}
import mtest.spark.sparkSession
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.spark.injection.*
import java.sql.Date
import java.time.*
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.DurationInt
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
  val root = "./data/test/spark/database/postgres/"

  implicit val ss: SparkSession = sparkSession

  val codec: AvroCodec[DBTable] = AvroCodec[DBTable]
  implicit val te: TypedEncoder[DBTable] = shapeless.cachedImplicit
  implicit val te2: TypedEncoder[PartialDBTable] = shapeless.cachedImplicit
  implicit val re: RowEncoder[DBTable] = shapeless.cachedImplicit
  implicit val rd: RowDecoder[DBTable] = shapeless.cachedImplicit
  val ate: SchematizedEncoder[DBTable] = SchematizedEncoder[DBTable](te, codec)

  val listener = SparkContextListener[IO](ss.sparkContext).debug()

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
    HikariTransactor.fromHikariConfig[IO](DBConfig(postgres).set(_.setMaximumPoolSize(4)).hikariConfig)

  pg.use(txn => (DBTable.drop *> DBTable.create).transact(txn)).unsafeRunSync()

  val hikari: HikariConfig = DBConfig(postgres).hikariConfig
  val loader: LoadTable[DBTable] = ss.loadTable(ate)

  test("load data") {
    val tds = TypedDataset.create(List(dbData))
    val d1 = loader.data(List(dbData))
    val d2 = loader.data(tds)
    val d3 = loader.data(tds.dataset)
    val d4 = loader.data(tds.dataset.rdd)
    assert(d1.diff(d2).dataset.count() == 0)
    assert(d3.diff(d4).dataset.count() == 0)
    fs2.Stream
      .eval(IO.sleep(2.seconds) >> IO(d1.diff(d2).dataset))
      .concurrently(listener)
      .compile
      .drain
      .unsafeRunSync()

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
    loader.jdbc(hikari, "sparktest").output.avro(root / "base").run[IO].unsafeRunSync()

    loader.avro(root / "base").output.avro(root / "avro" / 1).run[IO].unsafeRunSync()
    loader.avro(root / "base").output.binAvro(root / "bin.avro" / 1).run[IO].unsafeRunSync()
    loader.avro(root / "base").output.jackson(root / "jackson" / 1).run[IO].unsafeRunSync()
    loader.avro(root / "base").output.circe(root / "circe" / 1).run[IO].unsafeRunSync()
    loader
      .avro(root / "base")
      .output
      .kantan(root / "kantan" / 1, CsvConfiguration.rfc)
      .run[IO]
      .unsafeRunSync()
    loader.avro(root / "base").output.parquet(root / "parquet" / 1).run[IO].unsafeRunSync()
    loader.avro(root / "base").output.objectFile(root / "obj" / 1).run[IO].unsafeRunSync()

    val avro = loader.avro(root / "avro" / 1)
    val binAvro = loader.binAvro(root / "bin.avro" / 1)
    val jackson = loader.jackson(root / "jackson" / 1)
    val circe = loader.circe(root / "circe" / 1)
    val kantan = loader.kantan(root / "kantan" / 1, CsvConfiguration.rfc)
    val parquet = loader.parquet(root / "parquet" / 1)
    val obj = loader.objectFile(root / "obj" / 1)

    assert(avro.diff(binAvro).dataset.count() == 0)
    assert(jackson.diff(circe).dataset.count() == 0)
    assert(kantan.diff(parquet).dataset.count() == 0)
    assert(kantan.diff(obj).dataset.count() == 0)
  }

  test("spark") {
    val parquet = root / "spark" / "parquet"
    val json = root / "spark" / "json"
    val csv = root / "spark" / "csv"
    val avro = root / "spark" / "avro"

    loader.avro(root / "base").dataset.write.mode(SaveMode.Overwrite).parquet(parquet.toString())

    loader.avro(root / "base").dataset.write.mode(SaveMode.Overwrite).json(json.toString())

    loader.avro(root / "base").dataset.write.mode(SaveMode.Overwrite).csv(csv.toString())
    loader.avro(root / "base").dataset.write.mode(SaveMode.Overwrite).format("avro").save(avro.toString())

    assert(
      loader.spark.csv(csv, CsvConfiguration.rfc).diff(loader.spark.json(json)).dataset.count()
        == 0)
    assert(
      loader.spark.avro(avro).diff(loader.spark.parquet(parquet)).dataset.count()
        == 0)
  }

}
