package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class TextTest extends AnyFunSuite {
  import TabletData.*
  def saver(path: NJPath) = new RddFileHoarder[IO, Tablet](rdd).text(path)
  test("tablet") {
    val path = NJPath("./data/test/spark/persist/text/tablet/show-case-class")
    saver(path).errorIfExists.ignoreIfExists.overwrite.run.unsafeRunSync()
  }
  test("tablet - with new suffix") {
    val path = NJPath("./data/test/spark/persist/text/tablet/new-suffix")
    saver(path).withSuffix(".text").uncompress.run.unsafeRunSync()
  }

  test("tablet - deflate, compress-1") {
    val path = NJPath("./data/test/spark/persist/text/tablet/deflate")
    saver(path).deflate(5).run.unsafeRunSync()
  }

  test("tablet - gzip, compress-2") {
    val path = NJPath("./data/test/spark/persist/text/tablet/gzip")
    saver(path).gzip.run.unsafeRunSync()
  }

  test("tablet - bzip2, compress-3") {
    val path = NJPath("./data/test/spark/persist/text/tablet/bzip2")
    saver(path).bzip2.run.unsafeRunSync()
  }

  test("tablet - append") {
    val path = NJPath("./data/test/spark/persist/text/tablet/append")
    val t1 =
      try sparkSession.read.text(path.pathStr).count()
      catch { case _: Throwable => 0 }
    saver(path).append.run.unsafeRunSync()
    val t2 = sparkSession.read.text(path.pathStr).count()

    assert((t1 + rdd.count()) == t2)
  }
}
