package mtest.guard

import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import munit.FunSuite

class SyntaxTest extends FunSuite {

  private val service: ServiceGuard[IO] = TaskGuard[IO]("syntax").service("syntax")

  test("1.facilitate") {
    service.eventStreamR(_.facilitate("syntax")(_.activeGauge("active")))
  }

  test("2.logger") {
    service.eventStream(_.logger.good("ok"))
  }

  test("3.adhoc") {
    service.eventStream(_.adhoc.report)
  }

  test("4.tick") {
    service.eventStreamS(_.tickScheduled(_.empty))
  }
}
