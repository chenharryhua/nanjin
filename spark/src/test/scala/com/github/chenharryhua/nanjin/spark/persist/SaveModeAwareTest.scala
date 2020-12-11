package com.github.chenharryhua.nanjin.spark.persist

import better.files._
import cats.effect.IO
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite

class SaveModeAwareTest extends AnyFunSuite {
  import mtest.spark._
  test("not support append") {
    val sma = new SaveModeAware[IO](SaveMode.Append, "./data", sparkSession)
    assertThrows[Exception](sma.checkAndRun(blocker)(IO(())).unsafeRunSync())
  }

  test("error if exists") {
    val sma = new SaveModeAware[IO](SaveMode.ErrorIfExists, "./data", sparkSession)
    assertThrows[Exception](sma.checkAndRun(blocker)(IO(())).unsafeRunSync())
  }

  test("ignore if exists") {
    val sma = new SaveModeAware[IO](SaveMode.Ignore, "./data", sparkSession)
    sma.checkAndRun(blocker)(IO(())).unsafeRunSync()
  }

  test("overwrite if exists") {
    val path = "./data/test/spark/sma/overwrite.json"
    val file = File(path)
    file.createFileIfNotExists(true).overwrite("hello")
    val sma = new SaveModeAware[IO](SaveMode.Overwrite, path, sparkSession)
    sma.checkAndRun(blocker)(IO(file.overwrite("world")).void).unsafeRunSync()
    assert(file.contentAsString == "world")
  }
}
