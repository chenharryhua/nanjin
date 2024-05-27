package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.retrieveRatioGauge
import org.scalatest.funsuite.AnyFunSuite

class RatioTest extends AnyFunSuite {
  private val service = TaskGuard[IO]("ratio").service("ratio")

  test("1. init") {
    val mr = service.eventStream { ga =>
      ga.ratio("init").use(_ => ga.metrics.report)
    }.map(checkJson).mapFilter(metricReport).compile.lastOrError.unsafeRunSync()

    assert(retrieveRatioGauge(mr.snapshot.gauges).head._2.isEmpty)
  }

  test("2. zero denominator") {
    val mr = service.eventStream { ga =>
      ga.ratio("zero").use(r => r.incDenominator(0) >> ga.metrics.report)
    }.map(checkJson).mapFilter(metricReport).compile.lastOrError.unsafeRunSync()

    assert(retrieveRatioGauge(mr.snapshot.gauges).head._2.isEmpty)
  }

  test("3. 50%") {
    val mr = service.eventStream { ga =>
      ga.ratio("50%").use(r => r.incBoth(50, 100) >> ga.metrics.report)
    }.map(checkJson).mapFilter(metricReport).compile.lastOrError.unsafeRunSync()

    assert(retrieveRatioGauge(mr.snapshot.gauges).head._2.get == 50.0)
  }

  test("4. decrease") {
    val mr = service.eventStream { ga =>
      ga.ratio("decrease").use(r => r.incBoth(100, 100) >> r.incNumerator(-50) >> ga.metrics.report)
    }.map(checkJson).mapFilter(metricReport).compile.lastOrError.unsafeRunSync()

    assert(retrieveRatioGauge(mr.snapshot.gauges).head._2.get == 50.0)
  }

  test("5. negative") {
    val mr = service.eventStream { ga =>
      ga.ratio("negative").use(r => r.incBoth(0, 100) >> r.incNumerator(-50) >> ga.metrics.report)
    }.map(checkJson).mapFilter(metricReport).compile.lastOrError.unsafeRunSync()

    assert(retrieveRatioGauge(mr.snapshot.gauges).head._2.get == -50.0)
  }
}
