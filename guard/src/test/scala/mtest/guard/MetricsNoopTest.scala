package mtest.guard

import cats.Id
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.metrics.api.{Counter, Histogram, Meter, Timer}
import com.github.chenharryhua.nanjin.guard.metrics.api.gauges.{
  ActiveGauge,
  BalanceGauge,
  IdleGauge,
  Percentile
}
import org.scalatest.funsuite.AnyFunSuite

class MetricsNoopTest extends AnyFunSuite {

  // Counter noop

  test("1.Counter.noop inc is a no-op") {
    val counter = Counter.noop[IO]
    counter.inc(100).unsafeRunSync()
    counter.inc(1).unsafeRunSync()
  }

  test("2.Counter.noop unsafeInc is a no-op") {
    val counter = Counter.noop[IO]
    counter.unsafeInc(100)
    counter.unsafeInc(1)
  }

  test("3.Counter.noop works with Id") {
    val counter = Counter.noop[Id]
    counter.inc(10)
  }

  // Meter noop

  test("4.Meter.noop mark is a no-op") {
    val meter = Meter.noop[IO]
    meter.mark(100).unsafeRunSync()
    meter.mark(1).unsafeRunSync()
  }

  test("5.Meter.noop unsafeMark is a no-op") {
    val meter = Meter.noop[IO]
    meter.unsafeMark(100)
    meter.unsafeMark(1)
  }

  test("6.Meter.noop works with Id") {
    val meter = Meter.noop[Id]
    meter.mark(10)
  }

  // Histogram noop

  test("7.Histogram.noop update is a no-op") {
    val histogram = Histogram.noop[IO]
    histogram.update(100).unsafeRunSync()
    histogram.update(1).unsafeRunSync()
  }

  test("8.Histogram.noop unsafeUpdate is a no-op") {
    val histogram = Histogram.noop[IO]
    histogram.unsafeUpdate(100)
    histogram.unsafeUpdate(1)
  }

  test("9.Histogram.noop works with Id") {
    val histogram = Histogram.noop[Id]
    histogram.update(10)
  }

  // Timer noop

  test("10.Timer.noop elapsedNano is a no-op") {
    val timer = Timer.noop[IO]
    timer.elapsedNano(1000000).unsafeRunSync()
  }

  test("11.Timer.noop timing passes through the effect") {
    val timer = Timer.noop[IO]
    val result = timer.timing(IO.pure(42)).unsafeRunSync()
    assert(result == 42)
  }

  test("12.Timer.noop unsafeElapsedNano is a no-op") {
    val timer = Timer.noop[IO]
    timer.unsafeElapsedNano(1000000)
  }

  test("13.Timer.noop works with Id") {
    val timer = Timer.noop[Id]
    timer.elapsedNano(100)
    val result = timer.timing(42)
    assert(result == 42)
  }

  // Percentile noop

  test("14.Percentile.noop incNumerator is a no-op") {
    val percentile = Percentile.noop[IO]
    percentile.incNumerator(10).unsafeRunSync()
  }

  test("15.Percentile.noop incDenominator is a no-op") {
    val percentile = Percentile.noop[IO]
    percentile.incDenominator(10).unsafeRunSync()
  }

  test("16.Percentile.noop incBoth is a no-op") {
    val percentile = Percentile.noop[IO]
    percentile.incBoth(3, 4).unsafeRunSync()
  }

  test("17.Percentile.noop works with Id") {
    val percentile = Percentile.noop[Id]
    percentile.incNumerator(1)
    percentile.incDenominator(2)
    percentile.incBoth(3, 4)
  }

  // IdleGauge noop

  test("18.IdleGauge.noop wakeUp is a no-op") {
    val idle = IdleGauge.noop[IO]
    idle.wakeUp.unsafeRunSync()
  }

  test("19.IdleGauge.noop works with Id") {
    val idle = IdleGauge.noop[Id]
    idle.wakeUp
  }

  // ActiveGauge noop

  test("20.ActiveGauge.noop deactivate is a no-op") {
    val active = ActiveGauge.noop[IO]
    active.deactivate.unsafeRunSync()
  }

  test("21.ActiveGauge.noop works with Id") {
    val active = ActiveGauge.noop[Id]
    active.deactivate
  }

  // BalanceGauge noop

  test("22.BalanceGauge.noop forward is a no-op") {
    val balance = BalanceGauge.noop[IO, Int]
    balance.forward(100).unsafeRunSync()
  }

  test("23.BalanceGauge.noop backward is a no-op") {
    val balance = BalanceGauge.noop[IO, Int]
    balance.backward(50).unsafeRunSync()
  }

  test("24.BalanceGauge.noop works with Id") {
    val balance = BalanceGauge.noop[Id, Long]
    balance.forward(10L)
    balance.backward(5L)
  }
}
