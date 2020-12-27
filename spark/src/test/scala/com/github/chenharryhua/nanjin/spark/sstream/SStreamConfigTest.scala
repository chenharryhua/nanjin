package com.github.chenharryhua.nanjin.spark.sstream

import com.github.chenharryhua.nanjin.datetime.{sydneyTime, utcTime, NJDateTimeRange}
import org.scalatest.funsuite.AnyFunSuite

class SStreamConfigTest extends AnyFunSuite {
  val cfg = SStreamConfig(NJDateTimeRange(utcTime))

  test("checkpoint append") {
    assert(cfg.evalConfig.checkpoint.value == "./data/checkpoint/sstream")
    assert(
      cfg
        .withCheckpointAppend("abc")
        .withCheckpointAppend("xyz")
        .evalConfig
        .checkpoint
        .value == "./data/checkpoint/sstream/abc/xyz")
    assert(
      cfg
        .withCheckpointAppend("/abc")
        .withCheckpointAppend("xyz")
        .evalConfig
        .checkpoint
        .value == "./data/checkpoint/sstream/abc/xyz")
    assert(
      cfg
        .withCheckpointAppend("abc/")
        .withCheckpointAppend("/xyz")
        .evalConfig
        .checkpoint
        .value == "./data/checkpoint/sstream/abc/xyz")
    assert(
      cfg
        .withCheckpointAppend("/abc/")
        .withCheckpointAppend("/xyz/")
        .evalConfig
        .checkpoint
        .value == "./data/checkpoint/sstream/abc/xyz/")

    assertThrows[Exception](
      cfg
        .withCheckpointAppend("a c")
        .evalConfig
        .checkpoint
        .value == "./data/checkpoint/sstream/abc/xyz/")
  }

  test("checkpoint replace") {
    assert(cfg.withCheckpointReplace("./abc").evalConfig.checkpoint.value == "./abc")
  }
}
