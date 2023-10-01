package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.syntax.EncoderOps
import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

// sbt "guard/testOnly mtest.guard.PerformanceTest"

/** last time: (run more than once, pick up the best)
  *
  * silent.time.count: 1301K/s
  *
  * aware.time.count: 333K/s
  *
  * aware.time.count.notes: 429K/s
  *
  * notice.time.count: 323K/s
  *
  * notice.time.count.notes: 318K/s
  *
  * silent.time: 1193K/s
  *
  * aware.time: 459K/s
  *
  * aware.time.notes: 431K/s
  *
  * notice.time: 298K/s
  *
  * notice.time.notes: 313K/s
  *
  * silent.count: 2015K/s
  *
  * aware.count: 499K/s
  *
  * aware.count.notes: 469K/s
  *
  * notice.count: 335K/s
  *
  * notice.count.notes: 332K/s
  *
  * silent: 2017K/s
  *
  * aware: 481K/s
  *
  * aware.notes: 465K/s
  *
  * notice: 332K/s
  *
  * notice.notes: 305K/s
  */
@Ignore
class PerformanceTest extends AnyFunSuite {
  val service: ServiceGuard[IO] =
    TaskGuard[IO]("performance").service("actions").withMetricReport(policies.crontab(cron_1second))
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

  test("silent.time.count") {
    print("silent.time.count: ")
    var i = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.silent.withTiming.withCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("aware.time.count") {
    print("aware.time.count: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.unipartite.withCounting.withTiming).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("aware.time.count.notes") {
    print("aware.time.count.notes: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag
        .action("t", _.unipartite.withCounting.withTiming)
        .retry(IO(i += 1))
        .logOutput(_ => "aware.time.count.notes".asJson)
        .run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("notice.time.count") {
    print("notice.time.count: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.bipartite.withCounting.withTiming).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("notice.time.count.notes") {
    print("notice.time.count.notes: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag
        .action("t", _.bipartite.withCounting.withTiming)
        .retry(IO(i += 1))
        .logOutput(_ => "notice.time.count.notes".asJson)
        .run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("silent.time") {
    print("silent.time: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.silent.withTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("aware.time") {
    print("aware.time: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.unipartite.withTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }
  test("aware.time.notes") {
    print("aware.time.notes: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag
        .action("t", _.unipartite.withTiming.withoutCounting)
        .retry(IO(i += 1))
        .logOutput(_ => "aware.time.notes".asJson)
        .run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }
  test("notice.time") {
    print("notice.time: ")
    var i = 0
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.bipartite.withTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("notice.time.notes") {
    print("notice.time.notes: ")
    var i = 0
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.bipartite.withTiming.withoutCounting)
          .retry(IO(i += 1))
          .logOutput(_ => "notice.time.notes".asJson)
          .run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("silent.counting") {
    print("silent.count: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.silent.withoutTiming.withCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("aware.counting") {
    print("aware.count: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.unipartite.withoutTiming.withCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("aware.counting.notes") {
    print("aware.count.notes: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag
        .action("t", _.unipartite.withoutTiming.withCounting)
        .retry(IO(i += 1))
        .logOutput(_ => "aware.counting.notes".asJson)
        .run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("notice.counting") {
    print("notice.count: ")
    var i = 0
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.bipartite.withoutTiming.withCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("notice.counting.notes") {
    print("notice.count.notes: ")
    var i = 0
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.bipartite.withoutTiming.withCounting)
          .retry(IO(i += 1))
          .logOutput(_ => "notice.counting.notes".asJson)
          .run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("silent") {
    print("silent: ")
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
      val ts = ag.action("t", _.unipartite.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }
  test("aware.notes") {
    print("aware.notes: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag
        .action("t", _.unipartite.withoutTiming.withoutCounting)
        .retry(IO(i += 1))
        .logOutput(_ => "aware.notes".asJson)
        .run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }
  test("notice") {
    var i = 0
    print("notice: ")
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.bipartite.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("notice.notes") {
    var i = 0
    print("notice.notes: ")
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.bipartite.withoutTiming.withoutCounting)
          .retry(IO(i += 1))
          .logOutput(_ => "notice.notes".asJson)
          .run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

}
