package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import eu.timepit.refined.auto.*
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
@DoNotDiscover
class TextTest extends AnyFunSuite {
  import TabletData.*
  def saver(path: Url) = new RddFileHoarder[Tablet](rdd).text(path)
  val root             = "./data/test/spark/persist/text/tablet"
  test("tablet") {
    val path = root / "uncompressed"
    saver(path).run[IO].unsafeRunSync()
  }
  test("tablet - with new suffix") {
    val path = root / "new-suffix"
    saver(path).withSuffix("text").withCompression(_.Uncompressed).run[IO].unsafeRunSync()
  }

  test("tablet - deflate") {
    val path = root / "deflate5"
    saver(path).withCompression(_.Deflate(5)).run[IO].unsafeRunSync()
  }

  test("tablet - gzip") {
    val path = root / "gzip"
    saver(path).withCompression(_.Gzip).run[IO].unsafeRunSync()
  }

  test("tablet - bzip2") {
    val path = root / "bzip2"
    saver(path).withCompression(_.Bzip2).run[IO].unsafeRunSync()
  }

  test("tablet - lz4") {
    val path = root / "lz4"
    saver(path).withCompression(_.Lz4).run[IO].unsafeRunSync()
  }

  test("tablet - snappy") {
    val path = root / "snappy"
    saver(path).withCompression(_.Snappy).run[IO].unsafeRunSync()
  }

  test("tablet - append") {
    val path = root / "append"
    val t1   =
      try sparkSession.read.text(path.toString()).count()
      catch { case _: Throwable => 0 }
    saver(path).withSaveMode(_.Append).run[IO].unsafeRunSync()
    val t2 = sparkSession.read.text(path.toString()).count()

    assert(t1 + rdd.count() == t2)
  }
}
