package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.RddFileHoarder
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class TextTest extends AnyFunSuite {
  import TabletData._
  test("tablet") {
    val path  = "./data/test/spark/persist/text/tablet/show-case-class"
    val saver = RddFileHoarder[IO, Tablet](rdd, path)
    saver.text.folder.run(blocker).unsafeRunSync()
  }
  test("tablet - with new suffix") {
    val path  = "./data/test/spark/persist/text/tablet/new-suffix"
    val saver = RddFileHoarder[IO, Tablet](rdd, path)
    saver.text.folder.withSuffix(".text").run(blocker).unsafeRunSync()
  }
  test("tablet - gzip") {
    val path  = "./data/test/spark/persist/text/tablet/gzip"
    val saver = RddFileHoarder[IO, Tablet](rdd, path)
    saver.text.folder.gzip.run(blocker).unsafeRunSync()
  }
  test("tablet - deflate") {
    val path  = "./data/test/spark/persist/text/tablet/deflate"
    val saver = RddFileHoarder[IO, Tablet](rdd, path)
    saver.text.folder.deflate(5).run(blocker).unsafeRunSync()
  }
}
