package mtest.guard

import cats.Id
import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.metrics.api.{Counter, Histogram, Meter, Timer}
import com.github.chenharryhua.nanjin.guard.metrics.api.gauges.{ActiveGauge, IdleGauge, Ratio}
import munit.CatsEffectSuite

class MetricsNoopTest extends CatsEffectSuite {

  // Counter noop

  test("1.Counter.noop inc is a no-op") {
    val counter = Counter.noop[IO]
    counter.inc(100) >> counter.inc(1)
  }

  test("2.Counter.noop works with Id") {
    val counter = Counter.noop[Id]
    counter.inc(10)
  }

  // Meter noop

  test("3.Meter.noop mark is a no-op") {
    val meter = Meter.noop[IO]
    meter.mark(100) >> meter.mark(1)
  }

  test("4.Meter.noop works with Id") {
    val meter = Meter.noop[Id]
    meter.mark(10)
  }

  // Histogram noop

  test("5.Histogram.noop update is a no-op") {
    val histogram = Histogram.noop[IO]
    histogram.update(100) >> histogram.update(1)
  }

  test("6.Histogram.noop works with Id") {
    val histogram = Histogram.noop[Id]
    histogram.update(10)
  }

  // Timer noop

  test("7.Timer.noop elapsedNano is a no-op") {
    val timer = Timer.noop[IO]
    timer.elapsedNano(1000000)
  }

  test("8.Timer.noop timing passes through the effect") {
    val timer = Timer.noop[IO]
    timer.timing(IO.pure(42)).map(result => assert(result == 42))
  }

  test("9.Timer.noop works with Id") {
    val timer = Timer.noop[Id]
    timer.elapsedNano(100)
    val result = timer.timing(42)
    assert(result == 42)
  }

  // Ratio noop

  test("10.Ratio.noop incNumerator is a no-op") {
    val ratio = Ratio.noop[IO]
    ratio.incNumerator(10)
  }

  test("11.Ratio.noop incDenominator is a no-op") {
    val ratio = Ratio.noop[IO]
    ratio.incDenominator(10)
  }

  test("12.Ratio.noop incBoth is a no-op") {
    val ratio = Ratio.noop[IO]
    ratio.incBoth(3, 4)
  }

  test("13.Ratio.noop works with Id") {
    val ratio = Ratio.noop[Id]
    ratio.incNumerator(1)
    ratio.incDenominator(2)
    ratio.incBoth(3, 4)
  }

  // IdleGauge noop

  test("14.IdleGauge.noop wakeUp is a no-op") {
    val idle = IdleGauge.noop[IO]
    idle.wakeUp
  }

  test("15.IdleGauge.noop works with Id") {
    val idle = IdleGauge.noop[Id]
    idle.wakeUp
  }

  // ActiveGauge noop

  test("16.ActiveGauge.noop deactivate is a no-op") {
    val active = ActiveGauge.noop[IO]
    active.deactivate
  }

  test("17.ActiveGauge.noop works with Id") {
    val active = ActiveGauge.noop[Id]
    active.deactivate
  }

}
