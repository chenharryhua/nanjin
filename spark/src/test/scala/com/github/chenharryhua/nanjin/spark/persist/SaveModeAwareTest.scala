package com.github.chenharryhua.nanjin.spark.persist

import better.files.*
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.lemonlabs.uri.typesafe.dsl.*
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite
class SaveModeAwareTest extends AnyFunSuite {
  import mtest.spark.*
  val hadoopConfig: Configuration = sparkSession.sparkContext.hadoopConfiguration

  test("error if exists") {
    val sma = new SaveModeAware[IO](SaveMode.ErrorIfExists, "./data", hadoopConfig)
    assertThrows[Exception](sma.checkAndRun(IO(())).unsafeRunSync())
  }

  test("ignore if exists") {
    val sma = new SaveModeAware[IO](SaveMode.Ignore, "./data", hadoopConfig)
    sma.checkAndRun(IO(())).unsafeRunSync()
  }

  test("overwrite if exists") {
    val path = "./data/test/spark/sma/overwrite.json"
    val file = File(path)
    file.createFileIfNotExists(true).overwrite("hello")
    val sma = new SaveModeAware[IO](SaveMode.Overwrite, path, hadoopConfig)
    sma.checkAndRun(IO(file.overwrite("world")).void).unsafeRunSync()
    assert(file.contentAsString == "world")
  }
}
