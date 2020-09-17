package mtest.spark

import java.time.Instant

import cats.effect.IO
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.persist.loaders
import frameless.TypedEncoder
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DecimalType, IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.scalatest.funsuite.AnyFunSuite

import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode

object AvroTypedEncoderTestData {

  val schemaText: String =
    """
      |{
      |  "type": "record",
      |  "name": "Lion",
      |  "namespace": "mtest.spark.AvroTypedEncoderTestData",
      |  "fields": [
      |    {
      |      "name": "index",
      |      "type": "int"
      |    },
      |    {
      |      "name": "a",
      |      "type": {
      |        "type": "long",
      |        "logicalType": "timestamp-millis"
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
      |    }
      |  ]
      |}
      |
      |""".stripMargin

  final case class Lion(index: Int, a: Instant, b: BigDecimal)

  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP

  implicit val codec: AvroCodec[Lion]      = AvroCodec[Lion](schemaText).right.get
  implicit val encoder: TypedEncoder[Lion] = shapeless.cachedImplicit
  implicit val ate: AvroTypedEncoder[Lion] = AvroTypedEncoder[Lion](codec)

  val now = Instant.now

  val lions: List[Lion] = List(
    Lion(1, now, BigDecimal("1234.567")),
    Lion(2, now, BigDecimal("1234.56789")),
    Lion(3, now, BigDecimal("0.123456789")),
    Lion(4, now, BigDecimal("0.10001")),
    Lion(5, now, BigDecimal("1.2345"))
  )

  val expected: List[Lion] = List(
    Lion(1, now, BigDecimal("1234.567")),
    Lion(2, now, BigDecimal("1234.568")),
    Lion(3, now, BigDecimal("0.123")),
    Lion(4, now, BigDecimal("0.1")),
    Lion(5, now, BigDecimal("1.235"))
  )

  val expectedSchema: StructType = StructType(
    List(
      StructField("index", IntegerType, false),
      StructField("a", TimestampType, false),
      StructField("b", DecimalType(7, 3), false)))

  val rdd: RDD[Lion] = sparkSession.sparkContext.parallelize(lions)
  import sparkSession.implicits._

  val ds: Dataset[Lion] = sparkSession.createDataset(lions)
  val df: DataFrame     = ds.toDF()
}

class AvroTypedEncoderTest extends AnyFunSuite {
  import AvroTypedEncoderTestData._

  test("normalize rdd") {
    val n = ate.normalize(rdd)
    assert(n.collect[IO]().unsafeRunSync().toSet == expected.toSet)
    assert(n.schema == expectedSchema)
  }
  test("normalize dataset") {
    val n = ate.normalize(ds)
    assert(n.collect[IO]().unsafeRunSync().toSet == expected.toSet)
    assert(n.schema == expectedSchema)
  }
  test("normalize dataframe") {
    val n = ate.normalizeDF(df)
    assert(n.collect[IO]().unsafeRunSync().toSet == expected.toSet)
    assert(n.schema == expectedSchema)
  }

  /*
   * data saved in its original form
   * loaders will normalize the data
   */
  test("loaded json should be normalized") {
    val path = "./data/test/spark/ate/json"
    ds.write.mode(SaveMode.Overwrite).json(path)
    val r = loaders.json[Lion](path, ate)
    assert(r.collect[IO]().unsafeRunSync().toSet == expected.toSet)
    assert(r.schema == expectedSchema)
  }
  test("loaded csv should be normalized") {
    val path = "./data/test/spark/ate/csv"
    ds.write.mode(SaveMode.Overwrite).csv(path)
    val r = loaders.csv[Lion](path, ate)
    assert(r.collect[IO]().unsafeRunSync().toSet == expected.toSet)
    assert(r.schema == expectedSchema)
  }
  test("loaded avro should be normalized") {
    val path = "./data/test/spark/ate/avro"
    ds.write.format("avro").mode(SaveMode.Overwrite).save(path)
    val r = loaders.avro[Lion](path, ate)
    assert(r.collect[IO]().unsafeRunSync().toSet == expected.toSet)
    assert(r.schema == expectedSchema)
  }
  test("loaded parquet should be normalized") {
    val path = "./data/test/spark/ate/parquet"
    ds.write.mode(SaveMode.Overwrite).parquet(path)
    val r = loaders.parquet[Lion](path, ate)
    assert(r.collect[IO]().unsafeRunSync().toSet == expected.toSet)
    assert(r.schema == expectedSchema)
  }

  test("empty set") {
    assert(ate.emptyDataset.count[IO].unsafeRunSync() == 0)
  }

}
