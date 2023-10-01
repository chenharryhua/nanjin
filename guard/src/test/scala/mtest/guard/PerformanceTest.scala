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
  * silent.time.count: 982K/s
  *
  * unipartite.time.count: 350K/s
  *
  * unipartite.time.count.notes: 420K/s
  *
  * bipartite.time.count: 295K/s
  *
  * bipartite.time.count.notes: 307K/s
  *
  * silent.time: 1123K/s
  *
  * unipartite.time: 465K/s
  *
  * unipartite.time.notes: 398K/s
  *
  * bipartite.time: 315K/s
  *
  * bipartite.time.notes: 311K/s
  *
  * silent.count: 1918K/s
  *
  * unipartite.count: 490K/s
  *
  * unipartite.count.notes: 452K/s
  *
  * bipartite.count: 322K/s
  *
  * bipartite.count.notes: 271K/s
  *
  * silent: 1767K/s
  *
  * unipartite: 456K/s
  *
  * unipartite.notes: 450K/s
  *
  * bipartite: 307K/s
  *
  * bipartite.notes: 316K/s
  */

@Ignore
class PerformanceTest extends AnyFunSuite {
  val service: ServiceGuard[IO] =
    TaskGuard[IO]("performance").service("actions").withMetricReport(policies.crontab(cron_1second))
  val take: FiniteDuration = 15.seconds

  def speed(i: Int): String = f"${i / (take.toSeconds * 1000)}%4dK/s"

  test("silent.time.count") {
    print("silent.time.count: ")
    var i = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.silent.withTiming.withCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("unipartite.time.count") {
    print("unipartite.time.count: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.unipartite.withCounting.withTiming).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("unipartite.time.count.notes") {
    print("unipartite.time.count.notes: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag
        .action("t", _.unipartite.withCounting.withTiming)
        .retry(IO(i += 1))
        .logOutput(_ => "unipartite.time.count.notes".asJson)
        .run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("bipartite.time.count") {
    print("bipartite.time.count: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.bipartite.withCounting.withTiming).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("bipartite.time.count.notes") {
    print("bipartite.time.count.notes: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag
        .action("t", _.bipartite.withCounting.withTiming)
        .retry(IO(i += 1))
        .logOutput(_ => "bipartite.time.count.notes".asJson)
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

  test("unipartite.time") {
    print("unipartite.time: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.unipartite.withTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }
  test("unipartite.time.notes") {
    print("unipartite.time.notes: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag
        .action("t", _.unipartite.withTiming.withoutCounting)
        .retry(IO(i += 1))
        .logOutput(_ => "unipartite.time.notes".asJson)
        .run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }
  test("bipartite.time") {
    print("bipartite.time: ")
    var i = 0
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.bipartite.withTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("bipartite.time.notes") {
    print("bipartite.time.notes: ")
    var i = 0
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.bipartite.withTiming.withoutCounting)
          .retry(IO(i += 1))
          .logOutput(_ => "bipartite.time.notes".asJson)
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

  test("unipartite.counting") {
    print("unipartite.count: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.unipartite.withoutTiming.withCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("unipartite.counting.notes") {
    print("unipartite.count.notes: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag
        .action("t", _.unipartite.withoutTiming.withCounting)
        .retry(IO(i += 1))
        .logOutput(_ => "unipartite.counting.notes".asJson)
        .run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("bipartite.counting") {
    print("bipartite.count: ")
    var i = 0
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.bipartite.withoutTiming.withCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("bipartite.counting.notes") {
    print("bipartite.count.notes: ")
    var i = 0
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.bipartite.withoutTiming.withCounting)
          .retry(IO(i += 1))
          .logOutput(_ => "bipartite.counting.notes".asJson)
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

  test("unipartite") {
    print("unipartite: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.unipartite.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }
  test("unipartite.notes") {
    print("unipartite.notes: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag
        .action("t", _.unipartite.withoutTiming.withoutCounting)
        .retry(IO(i += 1))
        .logOutput(_ => "unipartite.notes".asJson)
        .run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }
  test("bipartite") {
    var i = 0
    print("bipartite: ")
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.bipartite.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("bipartite.notes") {
    var i = 0
    print("bipartite.notes: ")
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.bipartite.withoutTiming.withoutCounting)
          .retry(IO(i += 1))
          .logOutput(_ => "bipartite.notes".asJson)
          .run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

}
