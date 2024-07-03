package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.retrieveRatioGauge
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

class RatioTest extends AnyFunSuite {
  private val service = TaskGuard[IO]("ratio").service("ratio")

  test("1. init") {
    val mr = service.eventStream { ga =>
      ga.ratio("init", _.withTranslator(_ => Json.Null)).use(_ => ga.metrics.report)
    }.map(checkJson).mapFilter(metricReport).compile.lastOrError.unsafeRunSync()

    assert(retrieveRatioGauge(mr.snapshot.gauges).nonEmpty)
  }

  test("2. zero denominator") {
    val mr = service.eventStream { ga =>
      ga.ratio("zero").use(r => r.incDenominator(0) >> ga.metrics.report)
    }.map(checkJson).mapFilter(metricReport).compile.lastOrError.unsafeRunSync()

    assert(retrieveRatioGauge(mr.snapshot.gauges).nonEmpty)
  }
}
