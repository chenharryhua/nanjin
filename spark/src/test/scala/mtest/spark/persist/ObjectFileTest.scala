package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import mtest.spark._

@DoNotDiscover
class ObjectFileTest extends AnyFunSuite {
  import TabletData._
  test("object file identity") {
    val path  = "./data/test/spark/persist/object/tablet.obj"
    val saver = new RddFileHoarder[IO, Tablet](ds.rdd)
    saver.objectFile(path).errorIfExists.ignoreIfExists.overwrite.outPath(path).run(blocker).unsafeRunSync()
    val t = loaders.rdd.objectFile[Tablet](path, sparkSession).collect().toSet
    assert(data.toSet == t)
  }
}
