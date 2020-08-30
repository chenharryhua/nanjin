package mtest.spark.saver

import java.sql.Timestamp
import java.time.Instant

import com.github.chenharryhua.nanjin.spark.persist.{RawAvroLoader, RawAvroSaver}
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

object AvroTestData {

  val data: GoldenFish =
    GoldenFish(Instant.now, Timestamp.from(Instant.now()), BigDecimal("1234.56789"))
  val rdd: RDD[GoldenFish] = sparkSession.sparkContext.parallelize(List(data))
}

class AvroTest extends AnyFunSuite {
  import AvroTestData._
  val saver  = new RawAvroLoader[GoldenFish](GoldenFish.avroCodec.avroDecoder, sparkSession)
  val loader = new RawAvroSaver[GoldenFish]

  test("read/write identity") {}

}
