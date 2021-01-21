package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import mtest.spark._
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class TextTest extends AnyFunSuite {
  import TabletData._
  test("tablet") {
    val path  = "./data/test/spark/persist/text/tablet/show-case-class"
    val saver = new RddFileHoarder[IO, Tablet](rdd)
    saver.text(path).folder.errorIfExists.ignoreIfExists.overwrite.run(blocker).unsafeRunSync()
  }
  test("tablet - with new suffix") {
    val path  = "./data/test/spark/persist/text/tablet/new-suffix"
    val saver = new RddFileHoarder[IO, Tablet](rdd)
    saver.text(path).withSuffix(".text").folder.uncompress.run(blocker).unsafeRunSync()
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
  test("tablet - bzip2") {
    val path  = "./data/test/spark/persist/text/tablet/bzip2"
    val saver = new RddFileHoarder[IO, Tablet](rdd)
    saver.text(path).folder.bzip2.run(blocker).unsafeRunSync()
  }
  test("tablet - append") {
    val path = "./data/test/spark/persist/text/tablet/append"
    val t1 =
      try sparkSession.read.text(path).count()
      catch { case _: Throwable => 0 }
    val saver = new RddFileHoarder[IO, Tablet](rdd)
    saver.text(path).folder.append.run(blocker).unsafeRunSync()
    val t2 = sparkSession.read.text(path).count()

    assert((t1 + rdd.count()) == t2)
  }
}
