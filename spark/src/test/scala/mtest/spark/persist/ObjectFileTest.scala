package mtest.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import com.github.chenharryhua.nanjin.terminals.NJPath
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto.*

@DoNotDiscover
class ObjectFileTest extends AnyFunSuite {
  import TabletData.*
  test("object file identity") {
    val path  = NJPath("./data/test/spark/persist/object/tablet.obj")
    val saver = new RddFileHoarder[IO, Tablet](ds.rdd)
    saver.objectFile(path).errorIfExists.ignoreIfExists.overwrite.run.unsafeRunSync()
    val t = loaders.rdd.objectFile[Tablet](path, sparkSession).collect().toSet
    assert(data.toSet == t)
  }
}
