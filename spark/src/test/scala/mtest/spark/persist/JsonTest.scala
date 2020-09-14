package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import frameless.TypedDataset
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class JsonTest extends AnyFunSuite {

  test("rdd read/write identity") {
    import RoosterData._
    val path = "./data/test/spark/persist/json"
    delete(path)
    val saver = new RddFileHoarder[IO, Rooster](rdd,Rooster.avroCodec)
    saver.json(path).run(blocker).unsafeRunSync()
    val t: TypedDataset[Rooster] = loaders.json[Rooster](path,Rooster.ate)
    assert(expected == t.collect[IO]().unsafeRunSync().toSet)
  }
}
