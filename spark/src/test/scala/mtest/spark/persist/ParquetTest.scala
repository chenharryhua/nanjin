package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, savers}
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

class ParquetTest extends AnyFunSuite {
  import RoosterData._
  test("rdd read/write identity") {
    val path = "./data/test/spark/persist/parquet/raw"
    delete(path)
    savers.raw.parquet(rdd, path)
    val r: RDD[Rooster] = loaders.rdd.parquet[Rooster](path)
    assert(expected == r.collect().toSet)
  }

  test("tds read/write identity") {
    val path = "./data/test/spark/persist/parquet/spark"
    delete(path)
    savers.parquet(rdd, path)
    val t: TypedDataset[Rooster] = loaders.tds.parquet[Rooster](path)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
  }
}
