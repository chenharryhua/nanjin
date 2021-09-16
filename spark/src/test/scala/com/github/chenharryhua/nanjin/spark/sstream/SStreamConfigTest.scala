package com.github.chenharryhua.nanjin.spark.sstream

import com.github.chenharryhua.nanjin.datetime.{sydneyTime, utcTime, NJDateTimeRange}
import org.scalatest.funsuite.AnyFunSuite

class SStreamConfigTest extends AnyFunSuite {
  val cfg = SStreamConfig(utcTime)

  test("checkpoint") {}
}
