package mtest.guard

import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.funsuite.AnyFunSuite

class SyntaxTest extends AnyFunSuite {

  private val service: ServiceGuard[IO] = TaskGuard[IO]("syntax").service("syntax")

  test("facilitate") {
    service.eventStreamR(_.facilitate("syntax")(_.activeGauge("active")))
  }

  test("herald") {
    service.eventStream(_.herald.done("ok"))
  }

  test("adhoc") {
    service.eventStream(_.adhoc.report)
  }

  test("tick") {
    service.eventStreamS(_.ticks(_.giveUp))
    service.eventStreamS(_.tickImmediately(_.giveUp))
  }
}
