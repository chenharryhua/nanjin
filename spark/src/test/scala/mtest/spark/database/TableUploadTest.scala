package mtest.spark.database

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.database.TableName
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.*
import com.github.chenharryhua.nanjin.spark.database.{SparkDBTable, TableDef}
import frameless.{TypedDataset, TypedEncoder}
import mtest.spark.sparkSession
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

object TableUploadTestData {
  final case class Beaver(a: BigDecimal, c: Float, d: Double)

  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP

  val codec: NJAvroCodec[Beaver] = NJAvroCodec[Beaver](
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

  implicit val te: TypedEncoder[Beaver] = shapeless.cachedImplicit

  val table: SparkDBTable[IO, Beaver] =
    sparkDB.table(TableDef[Beaver](TableName("upload"), codec))

  val data: RDD[Beaver] = sparkSession.sparkContext.parallelize(
    List(
      Beaver(BigDecimal("12.3456"), Random.nextFloat(), Random.nextDouble()),
      Beaver(BigDecimal("123456"), Random.nextFloat(), Random.nextDouble()))
  )
  val tds = table.tableDef.ate.normalize(data, sparkSession)
}

class TableUploadTest extends AnyFunSuite {
  import TableUploadTestData.*

  test("upload") {
    tds.dbUpload(table).append.errorIfExists.ignoreIfExists.overwrite.withTableName("upload").run.unsafeRunSync()
    tds.rdd
      .dbUpload(table)
      .append
      .errorIfExists
      .ignoreIfExists
      .overwrite
      .withTableName("upload_rdd")
      .run
      .unsafeRunSync()
  }

  test("dump and reload") {
    table.dump.flatMap(_ => table.fromDisk.map(_.dataset.except(tds).count())).map(x => assert(x === 0)).unsafeRunSync()
  }
}
