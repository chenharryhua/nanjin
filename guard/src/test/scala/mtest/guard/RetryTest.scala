package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.common.chrono.zones.singaporeTime
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

final case class MyException() extends Exception("my exception")

class RetryTest extends AnyFunSuite {

  private val task: TaskGuard[IO] =
    TaskGuard[IO]("retry-guard")
      .updateConfig(_.withZoneId(singaporeTime))
      .updateConfig(_.withRestartPolicy(Policy.fixedDelay(1.seconds)))

  val policy: Policy = Policy.fixedDelay(1.seconds).limited(3)

  test("1.retry - completed trivial") {
    val Vector(s, c) = task
      .service("trivial")
      .eventStream { gd =>
        gd.facilitator("t").action(fun3 _).buildWith(identity).use(_.run((1, 1, 1)))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(c.isInstanceOf[ServiceStop])
  }

  test("2.retry - should retry 2 times when operation fail") {
    var i = 0
    val Vector(a, b, c, d) = task
      .service("2 times")
      .eventStream { gd =>
        gd.facilitator("t", _.withPolicy(policy))
          .action((_: Int) =>
            IO(if (i < 2) {
              i += 1; throw new Exception
            } else i))
          .buildWith(identity)
          .use(_.run(1))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServiceStop])
  }

  test("3.retry - should escalate to up level if retry failed") {
    val Vector(a, b, c, d, e) = task
      .service("escalate")
      .updateConfig(_.withRestartPolicy(Policy.giveUp))
      .eventStream { gd =>
        gd.facilitator("t", _.withPolicy(Policy.fixedDelay(1.seconds).limited(3)))
          .action((_: Int) => IO.raiseError[Int](new Exception("oops")))
          .buildWith(identity)
          .use(_.run(1))
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

  test("4.retry - Null pointer exception") {
    val List(a, b, c, d, e) = task
      .service("null exception")
      .updateConfig(_.withRestartPolicy(Policy.giveUp))
      .eventStream(ag =>
        ag.facilitator("t", _.withPolicy(policy))
          .action(IO.raiseError[Int](new NullPointerException))
          .buildWith(identity)
          .use(_.run(())))
      .map(checkJson)
      .take(6)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServiceMessage])
    assert(e.isInstanceOf[ServiceStop])
  }

  test("5.retry - isWorthRetry - should retry") {
    val Vector(a, b, c, d, e) = task
      .service("retry")
      .updateConfig(_.withRestartPolicy(Policy.giveUp))
      .eventStream { gd =>
        gd.facilitator("t", _.withPolicy(Policy.fixedDelay(0.1.seconds).limited(3)))
          .action(IO.raiseError[Int](MyException()))
          .buildWith(_.worthRetry(_.isInstanceOf[MyException]))
          .use(_.run(()))
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

  test("6.retry - isWorthRetry - should not retry") {
    val Vector(a, b) = task
      .service("no retry")
      .updateConfig(_.withRestartPolicy(Policy.fixedDelay(1.hour)))
      .eventStream { gd =>
        gd.zonedNow >>
          gd.facilitator("t", _.withPolicy(Policy.fixedDelay(0.1.seconds).limited(3)))
            .action(IO.raiseError[Int](new Exception))
            .buildWith(_.worthRetry(x => x.isInstanceOf[MyException]))
            .use(_.run(()))
      }
      .map(checkJson)
      .interruptAfter(2.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServicePanic])
  }

  test("7.cron policy") {
    val List(a, b, c, d, e) = task
      .service("cron")
      .updateConfig(_.withRestartPolicy(Policy.giveUp))
      .eventStream(
        _.facilitator("cron", _.withPolicy(Policy.crontab(_.secondly).limited(3)))
          .action(IO.raiseError[Int](new Exception("oops")))
          .buildWith(identity)
          .use(_.run(())))
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServiceMessage])
    assert(e.isInstanceOf[ServiceStop])
  }
}
