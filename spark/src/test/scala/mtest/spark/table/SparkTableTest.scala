package mtest.spark.table

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.database.DBConfig
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.{RddExt, SparkSessionExt}
import com.zaxxer.hikari.HikariConfig
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import io.circe.generic.auto.*
import io.lemonlabs.uri.typesafe.dsl.*
import kantan.csv.java8.*
import io.scalaland.chimney.dsl.*
import kantan.csv.generic.*
import kantan.csv.{CsvConfiguration, RowDecoder, RowEncoder}
import mtest.spark.sparkSession
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

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
  val root = "./data/test/spark/database/postgres/"

  implicit val ss: SparkSession = sparkSession
  import ss.implicits.*

  val codec: AvroCodec[DBTable] = AvroCodec[DBTable]
  implicit val re: RowEncoder[DBTable] = shapeless.cachedImplicit
  implicit val rd: RowDecoder[DBTable] = shapeless.cachedImplicit

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

  test("save/load") {
    ss.loadData(List(dbData)).write.mode(SaveMode.Overwrite).format("avro").save((root / "base").toString())
    val avroRdd = ss.loadDataset[DBTable](root / "base").avro.rdd
    avroRdd.out.avro(root / "avro" / 1).run[IO].unsafeRunSync()
    avroRdd.out.binAvro(root / "bin.avro" / 1).run[IO].unsafeRunSync()
    avroRdd.out.jackson(root / "jackson" / 1).run[IO].unsafeRunSync()
    avroRdd.out.parquet(root / "parquet" / 1).run[IO].unsafeRunSync()
    avroRdd.output.circe(root / "circe" / 1).run[IO].unsafeRunSync()
    avroRdd.output.kantan(root / "kantan" / 1, CsvConfiguration.rfc).run[IO].unsafeRunSync()
    avroRdd.output.objectFile(root / "obj" / 1).run[IO].unsafeRunSync()

    val avro = ss.loadDataset[DBTable](root / "avro" / 1).avro
    val binAvro = ss.loadDataset[DBTable](root / "bin.avro" / 1).binAvro
    val jackson = ss.loadDataset[DBTable](root / "jackson" / 1).jackson
    val circe = ss.loadDataset[DBTable](root / "circe" / 1).circe
    val kantan = ss.loadDataset[DBTable](root / "kantan" / 1).kantan(CsvConfiguration.rfc)
    val parquet = ss.loadDataset[DBTable](root / "parquet" / 1).parquet
    val obj = ss.loadDataset[DBTable](root / "obj" / 1).objectFile

    assert(avro.except(binAvro).count() == 0)
    assert(jackson.except(circe).count() == 0)
    assert(kantan.except(parquet).count() == 0)
    assert(kantan.except(obj).count() == 0)
  }

  test("spark") {
    val parquet = root / "spark" / "parquet"
    val json = root / "spark" / "json"
    val csv = root / "spark" / "csv"
    val avro = root / "spark" / "avro"

    val avroDataset = ss.loadDataset[DBTable](root / "base").avro

    avroDataset.write.mode(SaveMode.Overwrite).parquet(parquet.toString())
    avroDataset.write.mode(SaveMode.Overwrite).json(json.toString())
    avroDataset.write.mode(SaveMode.Overwrite).csv(csv.toString())
    avroDataset.write.mode(SaveMode.Overwrite).format("avro").save(avro.toString())
  }
}
