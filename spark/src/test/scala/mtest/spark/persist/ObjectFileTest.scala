package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import mtest.spark._
import cats.effect.unsafe.implicits.global

@DoNotDiscover
class ObjectFileTest extends AnyFunSuite {
  import TabletData._
  test("object file identity") {
    val path  = "./data/test/spark/persist/object/tablet.obj"
    val saver = new RddFileHoarder[IO, Tablet](ds.rdd)
    saver.objectFile(path).errorIfExists.ignoreIfExists.overwrite.run.unsafeRunSync()
    val t = loaders.rdd.objectFile[Tablet](path, sparkSession).collect().toSet
    assert(data.toSet == t)
  }
}
