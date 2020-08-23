package mtest.spark

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.saver.{RddFileLoader, RddFileSaver}
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import frameless.TypedEncoder
import kantan.csv.RowEncoder
import kantan.csv.generic._
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite
import frameless.cats.implicits._

import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode
import scala.util.Random
import cats.implicits._
import io.circe.Codec
import io.circe.generic.auto._

object DecimalTestData {
  final case class Duck(a: BigDecimal, b: BigDecimal, c: Int)

  val schemaText: String =
    """
      |{
      |  "type": "record",
      |  "name": "Duck",
      |  "namespace": "nj.spark",
      |  "fields": [
      |    {
      |      "name": "a",
      |      "type": {
      |        "type": "bytes",
      |        "logicalType": "decimal",
      |        "precision": 6,
      |        "scale": 2
      |      }
      |    },
      |    {
      |      "name": "b",
      |      "type": {
      |        "type": "bytes",
      |        "logicalType": "decimal",
      |        "precision": 7,
      |        "scale": 3
      |      }
      |    },
      |    {
      |      "name": "c",
      |      "type": "int"
      |    }
      |  ]
      |}
      |""".stripMargin

  val goodData: RDD[Duck] = sparkSession.sparkContext.parallelize(
    List(
      Duck(BigDecimal("1234.56"), BigDecimal("1234.567"), Random.nextInt()),
      Duck(BigDecimal("1234"), BigDecimal("12.34567"), Random.nextInt()),
      Duck(BigDecimal("1234.00"), BigDecimal("1234.0000001"), Random.nextInt())
    ))

  val schema: Schema = (new Schema.Parser).parse(schemaText)

  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP

  implicit val avroEncoder: Encoder[Duck] =
    shapeless.cachedImplicit[Encoder[Duck]].withSchema(SchemaFor[Duck](schema))
  implicit val typedEncoder: TypedEncoder[Duck] = shapeless.cachedImplicit
  implicit val rowEncoder: RowEncoder[Duck]     = shapeless.cachedImplicit
  implicit val circeCodec: Codec[Duck]          = io.circe.generic.semiauto.deriveCodec

  val saver = new RddFileSaver[IO, Duck](goodData)

  val loader = new RddFileLoader(sparkSession)

  implicit val avroDecoder: Decoder[Duck] =
    shapeless.cachedImplicit[Decoder[Duck]].withSchema(SchemaFor[Duck](schema))

}

class DecimalTest extends AnyFunSuite {
  import DecimalTestData._
  test("avro multi/single should be same") {
    val multi  = "./data/test/spark/decimal/avro/multi"
    val single = "./data/test/spark/decimal/avro/single.avro"

    val run = for {
      _ <- saver.avro(multi).multi.run(blocker)
      _ <- saver.avro(single).single.run(blocker)
      m <- loader.avro[Duck](multi).typedDataset.collect[IO]()
      s <- loader.avro[Duck](single).typedDataset.collect[IO]()
    } yield assert(m.toSet == s.toSet)
    run.unsafeRunSync()
  }
  test("parquet multi/single should be same") {
    val multi  = "./data/test/spark/decimal/parquet/multi"
    val single = "./data/test/spark/decimal/parquet/single.parquet"

    val run = for {
      _ <- saver.parquet(multi).multi.run(blocker)
      _ <- saver.parquet(single).single.run(blocker)
      m <- loader.parquet[Duck](multi).typedDataset.collect[IO]()
      s <- loader.parquet[Duck](single).typedDataset.collect[IO]()
    } yield assert(m.toSet == s.toSet)
    run.unsafeRunSync()
  }

  test("jackson multi/single should be same") {
    val multi  = "./data/test/spark/decimal/jackson/multi"
    val single = "./data/test/spark/decimal/jackson/single.json"

    val run = for {
      _ <- saver.jackson(multi).multi.run(blocker)
      _ <- saver.jackson(single).single.run(blocker)
      m <- loader.jackson[Duck](multi).typedDataset.collect[IO]()
      s <- loader.jackson[Duck](single).typedDataset.collect[IO]()
    } yield assert(m.toSet == s.toSet)
    run.unsafeRunSync()
  }

  test("circe multi/single should be same") {
    val multi  = "./data/test/spark/decimal/circe/multi"
    val single = "./data/test/spark/decimal/circe/single.json"

    val run = for {
      _ <- saver.circe(multi).multi.run(blocker)
      _ <- saver.circe(single).single.run(blocker)
      m <- loader.circe[Duck](multi).typedDataset.collect[IO]()
      s <- loader.circe[Duck](single).typedDataset.collect[IO]()
    } yield assert(m.toSet == s.toSet)
    run.unsafeRunSync()
  }

  test("csv multi/single should be same") {
    val multi  = "./data/test/spark/decimal/csv/multi"
    val single = "./data/test/spark/decimal/csv/single.csv"

    val run = for {
      _ <- saver.csv(multi).multi.run(blocker)
      _ <- saver.csv(single).single.run(blocker)
      m <- loader.csv[Duck](multi).typedDataset.collect[IO]()
      s <- loader.csv[Duck](single).typedDataset.collect[IO]()
    } yield assert(m.toSet == s.toSet)
    run.unsafeRunSync()
  }

}
