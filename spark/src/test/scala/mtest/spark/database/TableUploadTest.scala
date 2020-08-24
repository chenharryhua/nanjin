package mtest.spark.database

import cats.effect.IO
import com.github.chenharryhua.nanjin.database.TableName
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.database.{SparkTable, TableDef}
import com.sksamuel.avro4s.SchemaFor
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

object TableUploadTestData {
  final case class Beaver(a: BigDecimal, c: Float, d: Double)

  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP

  val schema = NJAvroCodec[Beaver](
    """
      |{
      |  "type": "record",
      |  "name": "Beaver",
      |  "namespace": "mtest.spark.database.TableUploadTestData",
      |  "fields": [
      |    {
      |      "name": "a",
      |      "type": {
      |        "type": "bytes",
      |        "logicalType": "decimal",
      |        "precision": 9,
      |        "scale": 3
      |      }
      |    },
      |    {
      |      "name": "c",
      |      "type": "float"
      |    },
      |    {
      |      "name": "d",
      |      "type": "double"
      |    }
      |  ]
      |}
      |""".stripMargin
  ).right.get

  val table: SparkTable[IO, Beaver] =
    TableDef[Beaver](TableName("upload"), schema).in[IO](postgres)

  val data: RDD[Beaver] = sparkSession.sparkContext.parallelize(
    List(
      Beaver(BigDecimal("12.3456"), Random.nextFloat(), Random.nextDouble()),
      Beaver(BigDecimal("123456"), Random.nextFloat(), Random.nextDouble()))
  )
}

class TableUploadTest extends AnyFunSuite {
  import TableUploadTestData._
  test("upload") {
    table.tableDataset(data).upload.overwrite.run.unsafeRunSync()
  }
}
