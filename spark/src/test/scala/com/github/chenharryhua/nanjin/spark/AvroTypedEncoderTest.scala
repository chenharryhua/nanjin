package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.injection.*
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.github.chenharryhua.nanjin.terminals.NJPath
import frameless.TypedEncoder
import mtest.spark.sparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.*
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto.*
import java.time.Instant
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

  val codec: NJAvroCodec[Lion]             = NJAvroCodec[Lion](schemaText).right.get
  implicit val encoder: TypedEncoder[Lion] = shapeless.cachedImplicit
  val ate: AvroTypedEncoder[Lion]          = AvroTypedEncoder[Lion](codec)

  val now: Instant = Instant.ofEpochMilli(Instant.now.toEpochMilli)

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
      StructField("index", IntegerType, nullable = false),
      StructField("a", TimestampType, nullable = false),
      StructField("b", DecimalType(7, 3), nullable = false)))

  val rdd: RDD[Lion] = sparkSession.sparkContext.parallelize(lions)
  import sparkSession.implicits.*

  val ds: Dataset[Lion] = sparkSession.createDataset(lions)
  val df: DataFrame     = ds.toDF()

}

class AvroTypedEncoderTest extends AnyFunSuite {
  import AvroTypedEncoderTestData.*

  test("normalize rdd") {
    val n = ate.normalize(rdd, sparkSession)
    assert(n.collect().toSet == expected.toSet)
    assert(n.schema == expectedSchema)
  }
  test("normalize dataset") {
    val n = ate.normalize(ds)
    assert(n.collect().toSet == expected.toSet)
    assert(n.schema == expectedSchema)
  }
  test("normalize dataframe") {
    val n = ate.normalizeDF(df)
    assert(n.collect().toSet == expected.toSet)
    assert(n.schema == expectedSchema)
  }

  /*
   * data saved in its original form
   * loaders will normalize the data
   */
  val root = NJPath("./data/test/spark/ate")

  test("loaded json should be normalized") {
    val path: NJPath = root / "json"
    ds.write.mode(SaveMode.Overwrite).json(path.pathStr)
    val r = loaders.json[Lion](path, ate, sparkSession)
    assert(r.collect().toSet == expected.toSet)
    assert(r.schema == expectedSchema)
  }
  test("loaded csv should be normalized") {
    val path = root / "csv"
    ds.write.mode(SaveMode.Overwrite).csv(path.pathStr)
    val r = loaders.csv[Lion](path, ate, sparkSession)
    assert(r.collect().toSet == expected.toSet)
    assert(r.schema == expectedSchema)
  }
  test("loaded avro should be normalized") {
    val path = root / "avro"
    ds.write.format("avro").mode(SaveMode.Overwrite).save(path.pathStr)
    val r = loaders.avro[Lion](path, ate, sparkSession)
    assert(r.collect().toSet == expected.toSet)
    assert(r.schema == expectedSchema)
  }
  test("loaded parquet should be normalized") {
    val path = root / "parquet"
    ds.write.mode(SaveMode.Overwrite).parquet(path.pathStr)
    val r = loaders.parquet[Lion](path, ate, sparkSession)
    assert(r.collect().toSet == expected.toSet)
    assert(r.schema == expectedSchema)
  }

  test("empty set") {
    val eds = ate.emptyDataset(sparkSession)
    assert(eds.count() == 0)
    assert(eds.schema == expectedSchema)
  }

  test("primitive type string") {
    val ate  = AvroTypedEncoder[String]
    val data = List("a", "b", "c", "d")
    val rdd  = sparkSession.sparkContext.parallelize(data)
    assert(ate.normalize(rdd, sparkSession).collect().toList == data)
  }

  test("primitive type int") {
    val ate           = AvroTypedEncoder[Int]
    val data          = List(1, 2, 3, 4)
    val rdd: RDD[Int] = sparkSession.sparkContext.parallelize(data)
    assert(ate.normalize(rdd, sparkSession).collect().toList == data)
  }

  test("primitive type array byte") {
    val ate                     = AvroTypedEncoder[Array[Byte]]
    val data: List[Array[Byte]] = List(Array(1), Array(2, 3), Array(4, 5, 6), Array(7, 8, 9, 10))
    val rdd                     = sparkSession.sparkContext.parallelize(data)
    assert(ate.normalize(rdd, sparkSession).collect().toList.flatten == data.flatten)
  }

  test("primitive type") {
    assert(AvroTypedEncoder[List[Int]].sparkSchema.head.name == "value")
  }

  test("other primitive types") {
    val ate1 = AvroTypedEncoder[Byte]
    val ate2 = AvroTypedEncoder[BigDecimal]
    val ate3 = AvroTypedEncoder[Float]
    val ate4 = AvroTypedEncoder[Double]
    val ate5 = AvroTypedEncoder[Long]
    val ate6 = AvroTypedEncoder[Boolean]
  }
}
