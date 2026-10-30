package mtest.common

import com.github.chenharryhua.nanjin.common.initial
import munit.FunSuite

import java.time.LocalDateTime

class InitialTest extends FunSuite {

  test("1.epoch constants have their expected dates") {
    assertEquals(initial.epoch, LocalDateTime.of(2019, 7, 21, 0, 0, 0))
    assertEquals(initial.kafkaEpoch, LocalDateTime.of(2012, 10, 23, 0, 0, 0))
    assertEquals(initial.sparkEpoch, LocalDateTime.of(2014, 2, 1, 0, 0, 0))
    assertEquals(initial.flinkEpoch, LocalDateTime.of(2014, 12, 1, 0, 0, 0))
  }
}
