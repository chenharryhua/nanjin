package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.console
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

class RatioTest extends AnyFunSuite {
  private val service = TaskGuard[IO]("ratio").service("ratio")

  test("1. init") {
    service.eventStream { ga =>
      ga.ratio("init", _.withTranslator(_ => Json.Null).withTag("completion_ratio"))
        .use(_ => ga.metrics.report)
    }.map(checkJson).mapFilter(metricReport).evalTap(console.json[IO]).compile.lastOrError.unsafeRunSync()
  }

  test("2. zero denominator") {
    service.eventStream { ga =>
      ga.ratio("zero").use(r => r.incDenominator(0) >> ga.metrics.report)
    }.map(checkJson).mapFilter(metricReport).evalTap(console.text[IO]).compile.lastOrError.unsafeRunSync()
  }
}
