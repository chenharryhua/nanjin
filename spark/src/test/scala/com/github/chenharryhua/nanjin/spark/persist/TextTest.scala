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
  def saver(path: NJPath) = new RddFileHoarder[IO, Tablet](IO(rdd)).text(path)
  val root                = NJPath("./data/test/spark/persist/text/tablet")
  test("tablet") {
    val path = root / "uncompressed"
    saver(path).errorIfExists.ignoreIfExists.overwrite.run.unsafeRunSync()
  }
  test("tablet - with new suffix") {
    val path = root / "new-suffix"
    saver(path).withSuffix("text").uncompressed.run.unsafeRunSync()
  }

  test("tablet - deflate") {
    val path = root / "deflate5"
    saver(path).deflate(5).run.unsafeRunSync()
  }

  test("tablet - gzip") {
    val path = root / "gzip"
    saver(path).gzip.run.unsafeRunSync()
  }

  test("tablet - bzip2") {
    val path = root / "bzip2"
    saver(path).bzip2.run.unsafeRunSync()
  }

  test("tablet - lz4") {
    val path = root / "lz4"
    saver(path).lz4.run.unsafeRunSync()
  }

  test("tablet - snappy") {
    val path = root / "snappy"
    saver(path).snappy.run.unsafeRunSync()
  }

  test("tablet - append") {
    val path = root / "append"
    val t1 =
      try sparkSession.read.text(path.pathStr).count()
      catch { case _: Throwable => 0 }
    saver(path).append.run.unsafeRunSync()
    val t2 = sparkSession.read.text(path.pathStr).count()

    assert(t1 + rdd.count() == t2)
  }
}
