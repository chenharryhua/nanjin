package mtest.spark.persist

import java.sql.Timestamp
import java.time.Instant

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, savers}
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

object AvroTestData {
  val instant: Instant = Instant.parse("2012-10-26T18:00:00Z")

  val data: List[GoldenFish] =
    List(
      GoldenFish(Instant.now, Timestamp.from(Instant.now()), BigDecimal("1234.567")),
      GoldenFish(instant, Timestamp.from(instant), BigDecimal("1234.567")),
      GoldenFish(Instant.now, Timestamp.from(Instant.now()), BigDecimal("1234.56789")),
      GoldenFish(instant, Timestamp.from(Instant.now()), BigDecimal("1234.56789")),
      GoldenFish(Instant.now, Timestamp.from(instant), BigDecimal("1234.567"))
    )

  val rdd: RDD[GoldenFish]         = sparkSession.sparkContext.parallelize(data)
  val ds: TypedDataset[GoldenFish] = TypedDataset.create(rdd)
}

class AvroTest extends AnyFunSuite {
  import AvroTestData._

  test("rdd read/write identity") {
    val path = "./data/test/spark/persist/avro/rdd"
    delete(path)
    savers.rdd.avro(rdd, path)
    val rst = loaders.rdd.avro[GoldenFish](path).collect().toSet
    assert(data.toSet == rst)
  }
  ignore("ds read/write identity") {
    val path = "./data/test/spark/persist/avro/tds"
    delete(path)
    savers.tds.avro(ds.dataset, path)
    val rst = loaders.tds.avro[GoldenFish](path).collect[IO]().unsafeRunSync().toSet
    assert(data.toSet == rst)
  }
}
