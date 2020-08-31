package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, savers}
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.scalatest.funsuite.AnyFunSuite

class JsonTest extends AnyFunSuite {

  test("rdd read/write identity") {
    import RoosterData._
    val path = "./data/test/spark/persist/json"
    delete(path)
    savers.json(rdd, path)
    val t: TypedDataset[Rooster] = loaders.tds.json[Rooster](path)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
  }
}
