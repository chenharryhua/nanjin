package mtest.spark.persist

import com.github.chenharryhua.nanjin.spark.persist.{loaders, savers}
import org.scalatest.funsuite.AnyFunSuite

class JacksonTest extends AnyFunSuite {
  test("rdd read/write identity") {
    import RoosterData._
    val path = "./data/test/spark/persist/jackson"
    delete(path)
    savers.raw.jackson(rdd, path)
    val r = loaders.raw.jackson[Rooster](path)
    assert(expected == r.collect().toSet)
  }
}
