package mtest.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.persist.RddFileHoarder
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class TextTest extends AnyFunSuite {
  import TabletData.*
  test("tablet") {
    val path  = "./data/test/spark/persist/text/tablet/show-case-class"
    val saver = new RddFileHoarder[IO, Tablet](rdd)
    saver.text(path).folder.errorIfExists.ignoreIfExists.overwrite.run.unsafeRunSync()
  }
  test("tablet - with new suffix") {
    val path  = "./data/test/spark/persist/text/tablet/new-suffix"
    val saver = new RddFileHoarder[IO, Tablet](rdd)
    saver.text(path).withSuffix(".text").folder.uncompress.run.unsafeRunSync()
  }
  test("tablet - single gzip") {
    val path  = "./data/test/spark/persist/text/tablet/tablet.txt.gz"
    val saver = new RddFileHoarder[IO, Tablet](rdd)
    saver.text(path).file.gzip.sink.compile.drain.unsafeRunSync()
  }

  test("tablet - single deflate") {
    val path  = "./data/test/spark/persist/text/tablet/tablet.txt.deflate"
    val saver = new RddFileHoarder[IO, Tablet](rdd)
    saver.text(path).file.deflate(1).sink.compile.drain.unsafeRunSync()
  }

  test("tablet - deflate, compress-1") {
    val path  = "./data/test/spark/persist/text/tablet/deflate"
    val saver = new RddFileHoarder[IO, Tablet](rdd)
    saver.text(path).folder.deflate(5).run.unsafeRunSync()
  }

  test("tablet - gzip, compress-2") {
    val path  = "./data/test/spark/persist/text/tablet/gzip"
    val saver = new RddFileHoarder[IO, Tablet](rdd)
    saver.text(path).folder.gzip.run.unsafeRunSync()
  }

  test("tablet - bzip2, compress-3") {
    val path  = "./data/test/spark/persist/text/tablet/bzip2"
    val saver = new RddFileHoarder[IO, Tablet](rdd)
    saver.text(path).folder.bzip2.run.unsafeRunSync()
  }

  test("tablet - append") {
    val path = "./data/test/spark/persist/text/tablet/append"
    val t1 =
      try sparkSession.read.text(path).count()
      catch { case _: Throwable => 0 }
    val saver = new RddFileHoarder[IO, Tablet](rdd)
    saver.text(path).folder.append.run.unsafeRunSync()
    val t2 = sparkSession.read.text(path).count()

    assert((t1 + rdd.count()) == t2)
  }
}
