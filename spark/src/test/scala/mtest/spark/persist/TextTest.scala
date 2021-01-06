package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.RddFileHoarder
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import mtest.spark._

@DoNotDiscover
class TextTest extends AnyFunSuite {
  import TabletData._
  test("tablet") {
    val path  = "./data/test/spark/persist/text/tablet/show-case-class"
    val saver = new RddFileHoarder[IO, Tablet](rdd)
    saver.text(path).errorIfExists.ignoreIfExists.overwrite.outPath(path).folder.run(blocker).unsafeRunSync()
  }
  test("tablet - with new suffix") {
    val path  = "./data/test/spark/persist/text/tablet/new-suffix"
    val saver = new RddFileHoarder[IO, Tablet](rdd)
    saver.text(path).folder.withSuffix(".text").run(blocker).unsafeRunSync()
  }
  test("tablet - gzip") {
    val path  = "./data/test/spark/persist/text/tablet/tablet.txt.gz"
    val saver = new RddFileHoarder[IO, Tablet](rdd)
    saver.text(path).file.gzip.run(blocker).unsafeRunSync()
  }
  test("tablet - deflate") {
    val path  = "./data/test/spark/persist/text/tablet/deflate"
    val saver = new RddFileHoarder[IO, Tablet](rdd)
    saver.text(path).folder.deflate(5).run(blocker).unsafeRunSync()
  }
}
