package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

// sbt "guard/testOnly mtest.guard.PerformanceTest"

/** last time: (run more than once, pick up the best)
  *
  * silent.time.count: 1000K/s
  *
  * unipartite.time.count: 399K/s
  *
  * unipartite.time.count.notes: 427K/s
  *
  * bipartite.time.count: 313K/s
  *
  * bipartite.time.count.notes: 343K/s
  *
  * silent.time: 1201K/s
  *
  * unipartite.time: 489K/s
  *
  * unipartite.time.notes: 448K/s
  *
  * bipartite.time: 341K/s
  *
  * bipartite.time.notes: 332K/s
  *
  * silent.count: 2001K/s
  *
  * unipartite.count: 549K/s
  *
  * unipartite.count.notes: 501K/s
  *
  * bipartite.count: 364K/s
  *
  * bipartite.count.notes: 343K/s
  *
  * silent: 2008K/s
  *
  * unipartite: 538K/s
  *
  * unipartite.notes: 503K/s
  *
  * bipartite: 353K/s
  *
  * bipartite.notes: 348K/s
  */

class PerformanceTest extends AnyFunSuite {
  val service: ServiceGuard[IO] =
    TaskGuard[IO]("performance").service("actions").withMetricReport(policies.crontab(cron_1second))
  val take: FiniteDuration = 3.seconds

  def speed(i: Int): String = f"${i / (take.toSeconds * 1000)}%4dK/s"

  test("silent.time.count") {
    print("silent.time.count: ")
    var i = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.silent.timed.counted).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("unipartite.time.count") {
    print("unipartite.time.count: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.unipartite.counted.timed).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("unipartite.time.count.notes") {
    print("unipartite.time.count.notes: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag
        .action("t", _.unipartite.counted.timed)
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
      val ts = ag.action("t", _.bipartite.counted.timed).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("bipartite.time.count.notes") {
    print("bipartite.time.count.notes: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag
        .action("t", _.bipartite.counted.timed)
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
      val ts = ag.action("t", _.silent.timed.uncounted).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("unipartite.time") {
    print("unipartite.time: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.unipartite.timed.uncounted).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }
  test("unipartite.time.notes") {
    print("unipartite.time.notes: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag
        .action("t", _.unipartite.timed.uncounted)
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
        ag.action("t", _.bipartite.timed.uncounted).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("bipartite.time.notes") {
    print("bipartite.time.notes: ")
    var i = 0
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.bipartite.timed.uncounted)
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
      val ts = ag.action("t", _.silent.untimed.counted).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("unipartite.counting") {
    print("unipartite.count: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.unipartite.untimed.counted).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("unipartite.counting.notes") {
    print("unipartite.count.notes: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag
        .action("t", _.unipartite.untimed.counted)
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
        ag.action("t", _.bipartite.untimed.counted).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("bipartite.counting.notes") {
    print("bipartite.count.notes: ")
    var i = 0
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.bipartite.untimed.counted)
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
      val ts = ag.action("t", _.silent.untimed.uncounted).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("unipartite") {
    print("unipartite: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.unipartite.untimed.uncounted).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }
  test("unipartite.notes") {
    print("unipartite.notes: ")
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag
        .action("t", _.unipartite.untimed.uncounted)
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
        ag.action("t", _.bipartite.untimed.uncounted).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

  test("bipartite.notes") {
    var i = 0
    print("bipartite.notes: ")
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.bipartite.untimed.uncounted)
          .retry(IO(i += 1))
          .logOutput(_ => "bipartite.notes".asJson)
          .run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))

  }

}
