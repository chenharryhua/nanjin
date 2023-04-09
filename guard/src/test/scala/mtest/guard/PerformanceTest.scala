package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import eu.timepit.refined.auto.*
import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

// sbt "guard/testOnly mtest.guard.PerformanceTest"

/** last time: (run more than once, pick up the best)
  *
  * time.count.silent: 775K/s
  *
  * time.count.aware: 392K/s
  *
  * time.count.notice: 337K/s
  *
  * time.silent: 655K/s
  *
  * time.aware: 423K/s
  *
  * time.notice: 332K/s
  *
  * count.silent: 745K/s
  *
  * count.aware: 467K/s
  *
  * count.notice: 376K/s
  *
  * silent: 964K/s
  *
  * aware: 486K/s
  *
  * notice: 380K/s
  */

@Ignore
class PerformanceTest extends AnyFunSuite {
  val service: ServiceGuard[IO] =
    TaskGuard[IO]("performance").service("actions").updateConfig(_.withMetricReport(cron_1second))
  val take: FiniteDuration = 15.seconds

  def speed(i: Int): String = f"${i / (take.toSeconds * 1000)}%4dK/s"

  ignore("alert") {
    print("alert:")
    var i = 0
    service.eventStream { ag =>
      val ts = ag.alert("alert").info("alert").map(_ => i += 1)
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("with trace") {
    print("trace:")
    var i = 0
    service.eventStream { ag =>
      val ts = ag.action("trace").retry(IO(i += 1))
      ag.root("root").use(s => ts.runWithSpan(s).foreverM.timeout(take)).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("silent.time.count") {
    print("time.count.silent:")
    var i = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.silent.withTiming.withCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("aware.time.count") {
    print("time.count.aware: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.aware.withCounting.withTiming).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("notice.time.count") {
    print("time.count.notice:")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.notice.withCounting.withTiming).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("silent.time") {
    print("time.silent:")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.silent.withTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("aware.time") {
    print("time.aware: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.aware.withTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("notice.time") {
    print("time.notice:")
    var i = 0
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.notice.withTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("silent.counting") {
    print("count.silent:")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.silent.withoutTiming.withCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("aware.counting") {
    print("count.aware: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.aware.withoutTiming.withCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("notice.counting") {
    print("count.notice:")
    var i = 0
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.notice.withoutTiming.withCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("silent") {
    print("silent:")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.silent.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("aware") {
    print("aware: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.aware.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("notice") {
    var i = 0
    print("notice:")
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.notice.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

}
