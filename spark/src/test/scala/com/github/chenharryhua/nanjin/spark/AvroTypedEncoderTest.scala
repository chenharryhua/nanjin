package com.github.chenharryhua.nanjin.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.persist.{loaders, saveRDD}
import com.github.chenharryhua.nanjin.terminals.{Compression, Hadoop}

import frameless.TypedEncoder
import io.lemonlabs.uri.typesafe.dsl.*
import mtest.spark.sparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.*
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
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

  val codec: AvroCodec[Lion] = AvroCodec[Lion](schemaText)
  implicit val encoder: TypedEncoder[Lion] = shapeless.cachedImplicit
  val ate: SchematizedEncoder[Lion] = SchematizedEncoder[Lion](codec)

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
  val df: DataFrame = ds.toDF()

}

class AvroTypedEncoderTest extends AnyFunSuite {
  import AvroTypedEncoderTestData.*
  val hdp: Hadoop[IO] = sparkSession.hadoop[IO]

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
  val root = "./data/test/spark/ate"

  test("loaded json should be normalized") {
    val path = root / "json"
    ds.write.mode(SaveMode.Overwrite).json(path.toString())
    val r = loaders.spark.json[Lion](path, sparkSession, ate)
    assert(r.collect().toSet == expected.toSet)
    assert(r.schema == expectedSchema)
  }

//  test("loaded csv should be normalized") {
//    val path = root / "csv"
//    ds.write.mode(SaveMode.Overwrite).csv(path.pathStr)
//    val r = loaders.csv[Lion](path, ate, sparkSession)
//    assert(r.collect().toSet == expected.toSet)
//    assert(r.schema == expectedSchema)
//  }

  test("loaded avro should be normalized") {
    val path = root / "spark" / "avro"
    ds.write.format("avro").mode(SaveMode.Overwrite).save(path.toString())
    val r = loaders.spark.avro[Lion](path, sparkSession, ate)
    assert(r.collect().toSet == expected.toSet)
    assert(r.schema == expectedSchema)
  }
  test("loaded parquet should be normalized") {
    val path = root / "spark" / "parquet"
    ds.write.mode(SaveMode.Overwrite).parquet(path.toString())
    val r = loaders.spark.parquet[Lion](path, sparkSession, ate)
    assert(r.collect().toSet == expected.toSet)
    assert(r.schema == expectedSchema)
  }

  test("loaded avro should be normalized - apache") {
    val path = root / "apache" / "avro"
    hdp.delete(path).unsafeRunSync()
    saveRDD.avro(rdd, path, ate.avroCodec, Compression.Uncompressed)
    val r = loaders.avro[Lion](path, sparkSession, ate)
    assert(r.collect().toSet == expected.toSet)
    assert(r.schema == expectedSchema)
  }
  test("loaded parquet should be normalized - apache") {
    val path = root / "apache" / "parquet"
    hdp.delete(path).unsafeRunSync()
    saveRDD.parquet(rdd, path, ate.avroCodec, Compression.Uncompressed)
    val r = loaders.parquet[Lion](path, sparkSession, ate)
    assert(r.collect().toSet == expected.toSet)
    assert(r.schema == expectedSchema)
  }

  test("empty set") {
    val eds = ate.emptyDataset(sparkSession)
    assert(eds.count() == 0)
    assert(eds.schema == expectedSchema)
  }

  test("primitive type string") {
    val ate = SchematizedEncoder[String]
    val data = List("a", "b", "c", "d")
    val rdd = sparkSession.sparkContext.parallelize(data)
    assert(ate.normalize(rdd, sparkSession).collect().toList == data)
  }

  test("primitive type int") {
    val ate = SchematizedEncoder[Int]
    val data = List(1, 2, 3, 4)
    val rdd: RDD[Int] = sparkSession.sparkContext.parallelize(data)
    assert(ate.normalize(rdd, sparkSession).collect().toList == data)
  }

  test("primitive type array byte") {
    val ate = SchematizedEncoder[Array[Byte]]
    val data: List[Array[Byte]] = List(Array(1), Array(2, 3), Array(4, 5, 6), Array(7, 8, 9, 10))
    val rdd = sparkSession.sparkContext.parallelize(data)
    assert(ate.normalize(rdd, sparkSession).collect().toList.flatten == data.flatten)
  }

  test("primitive type") {
    assert(SchematizedEncoder[List[Int]].sparkSchema.head.name == "value")
  }

  test("other primitive types") {
    SchematizedEncoder[Byte]
    SchematizedEncoder[BigDecimal]
    SchematizedEncoder[Float]
    SchematizedEncoder[Double]
    SchematizedEncoder[Long]
    SchematizedEncoder[Boolean]
  }
}
