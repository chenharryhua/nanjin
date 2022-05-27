package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

class ConfigTest extends AnyFunSuite {
  val service: ServiceGuard[IO] = TaskGuard[IO]("config").service("config")
  test("expensive") {
    val as = service.eventStream { agent =>
      agent.notice.expensive.run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(as.actionInfo.actionParams.isExpensive)
  }

  test("cheap") {
    val as = service.eventStream { agent =>
      agent.notice.cheap.run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(!as.actionInfo.actionParams.isExpensive)
  }

  test("counting") {
    val as = service.eventStream { agent =>
      agent.notice.updateConfig(_.withCounting).run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(as.actionInfo.actionParams.isCounting)
  }
  test("without counting") {
    val as = service.eventStream { agent =>
      agent.notice.updateConfig(_.withoutCounting).run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(!as.actionInfo.actionParams.isCounting)
  }

  test("timing") {
    val as = service.eventStream { agent =>
      agent.notice.updateConfig(_.withTiming).run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(as.actionInfo.actionParams.isTiming)
  }

  test("without timing") {
    val as = service.eventStream { agent =>
      agent.notice.updateConfig(_.withoutTiming).run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(!as.actionInfo.actionParams.isTiming)
  }

  test("notice") {
    val as = service.eventStream { agent =>
      agent.notice.run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(as.actionInfo.actionParams.isNotice)
  }

  test("critical") {
    val as = service.eventStream { agent =>
      agent.critical.run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(as.actionInfo.actionParams.isCritical)
  }
  test("trivial") {
    val as = service.eventStream { agent =>
      agent.trivial.run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync()
    assert(as.isEmpty)
  }
  test("normal") {
    val as = service.eventStream { agent =>
      agent.normal.run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync()
    assert(as.isEmpty)
  }
  test("max retries") {
    val as = service.eventStream { agent =>
      agent.notice.max(10).run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(as.actionInfo.actionParams.retry.maxRetries.value == 10)
  }
}
