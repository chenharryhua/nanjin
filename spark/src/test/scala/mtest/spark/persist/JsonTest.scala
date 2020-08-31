package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileSaver}
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.scalatest.funsuite.AnyFunSuite

class JsonTest extends AnyFunSuite {

  test("rdd read/write identity") {
    import RoosterData._
    val path = "./data/test/spark/persist/json"
    delete(path)
    val saver = new RddFileSaver[IO, Rooster](rdd)
    saver.json(path).run(blocker).unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.json[Rooster](path)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
  }
}
