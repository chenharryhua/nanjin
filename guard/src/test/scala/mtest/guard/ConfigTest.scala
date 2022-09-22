package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.config.NJRetryPolicy
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps

class ConfigTest extends AnyFunSuite {
  val service: ServiceGuard[IO] = TaskGuard[IO]("config").service("config")

  test("counting") {
    val as = service.eventStream { agent =>
      agent.action("cfg")(_.notice.withCounting).run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(as.actionInfo.actionParams.isCounting)
  }
  test("without counting") {
    val as = service.eventStream { agent =>
      agent.action("cfg")(_.notice.withoutCounting).run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(!as.actionInfo.actionParams.isCounting)
  }

  test("timing") {
    val as = service.eventStream { agent =>
      agent.action("cfg")(_.notice.withTiming).run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(as.actionInfo.actionParams.isTiming)
  }

  test("without timing") {
    val as = service.eventStream { agent =>
      agent.action("cfg")(_.notice.withoutTiming).run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(!as.actionInfo.actionParams.isTiming)
  }

  test("notice") {
    val as = service.eventStream { agent =>
      agent.action("cfg")(_.notice).run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(as.actionInfo.actionParams.isNotice)
  }

  test("critical") {
    val as = service.eventStream { agent =>
      agent.action("cfg")(_.critical).run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync().get.asInstanceOf[ActionStart]
    assert(as.actionInfo.actionParams.isCritical)
  }
  test("trivial") {
    val as = service.eventStream { agent =>
      agent.action("cfg")(_.trivial).run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync()
    assert(as.isEmpty)
  }

  test("silent") {
    val as = service.eventStream { agent =>
      agent.action("cfg")(_.silent).run(IO(1))
    }.filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync()
    assert(as.isEmpty)
  }

  test("retries") {
    val as = service.eventStream { agent =>
      agent.action("cfg")(_.withConstantDelay(1.second, 15).critical).run(IO(1))
    }.debug().filter(_.isInstanceOf[ActionStart]).compile.last.unsafeRunSync()

    assert(as.get.asInstanceOf[ActionStart].actionInfo.actionParams.retry.maxRetries.get.value === 15)
    assert(
      as.get.asInstanceOf[ActionStart].actionInfo.actionParams.retry.njRetryPolicy === NJRetryPolicy
        .ConstantDelay(1.seconds.toJava))

  }
}
