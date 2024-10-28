package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.chrono.Policy.*
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class CancellationTest extends AnyFunSuite {

  val task: TaskGuard[IO] =
    TaskGuard[IO]("cancellation").updateConfig(
      _.disableJmx.disableHttpServer
        .withZoneId(sydneyTime)
        .withMetricDailyReset
        .withRestartPolicy(fixedDelay(1.seconds)))

  val policy: Policy = crontab(_.secondly).limited(3)

  test("1.cancellation - canceled actions will not be retried") {
    val Vector(a, b) = task
      .service("failed")
      .updateConfig(_.withRestartPolicy(fixedDelay(1.hour)))
      .eventStream(ag =>
        ag.facilitator("canceled", _.withPolicy(policy))
          .action(IO(1) <* IO.canceled)
          .buildWith(identity)
          .use(_.run(())))
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceStop])
    assert(b.asInstanceOf[ServiceStop].cause.isInstanceOf[ServiceStopCause.ByCancellation.type])

  }

  test("2.cancellation - can be canceled externally") {
    val Vector(a, b) = task
      .service("externally")
      .updateConfig(_.withRestartPolicy(fixedDelay(1.hour)))
      .eventStream { ag =>
        val a1 = ag.facilitator("never").action(never_fun).buildWith(identity).use(_.run(()))
        IO.parSequenceN(2)(List(IO.sleep(2.second) >> IO.canceled, a1))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceStop])
    assert(b.asInstanceOf[ServiceStop].cause.exitCode == 2)
  }

  test("3.canceled by external exception") {
    val Vector(a, b) = task
      .service("external exception")
      .updateConfig(_.withRestartPolicy(giveUp))
      .eventStream { ag =>
        val a1 = ag.facilitator("never").action(never_fun).buildWith(identity).use(_.run(()))
        IO.parSequenceN(2)(List(IO.sleep(1.second) >> err_fun(1), a1))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceStop])
  }

  test("4.cancellation - wrapped within cancelable") {
    val Vector(a, b, c, d, e) = task
      .service("wrap")
      .updateConfig(_.withRestartPolicy(fixedDelay(1.hour)))
      .eventStream { ag =>
        val a1 = ag
          .facilitator("exception", _.withPolicy(policy))
          .action(IO.raiseError[Int](new Exception))
          .buildWith(identity)
          .use(_.run(()))
        IO.parSequenceN(2)(List(IO.sleep(2.second) >> IO.canceled, IO.uncancelable(_ => a1)))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServiceMessage])
    assert(e.isInstanceOf[ServiceStop])
  }

  test("5.cancellation - never can be canceled") {
    val Vector(a) =
      task
        .service("never")
        .updateConfig(_.withRestartPolicy(fixedDelay(1.hour)))
        .eventStream(_.facilitator("never").action(IO.never[Int]).buildWith(identity).use(_.run(())))
        .map(checkJson)
        .interruptAfter(2.seconds)
        .compile
        .toVector
        .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
  }
}
