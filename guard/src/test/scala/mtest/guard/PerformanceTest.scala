package mtest.guard

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
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
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("performance")
      .service("actions")
      .updateConfig(_.withMetricReport(policies.crontab(_.secondly)))

  private val take: FiniteDuration = 5.seconds

  private def speed(i: Int): String =
    f"${i / (take.toSeconds * 1000)}%4dK/s, ${fmt.format(take / i.toLong)}"

  test("warm-up") {
    print("warm-up: ")
    var i = 0
    service.eventStream { ga =>
      val run = for {
        action <- ga.action("t", _.unipartite).retry(IO(i += 1)).buildWith(identity)
        timer <- ga.timer("t")
      } yield Kleisli { (_: Unit) =>
        action.run(()).timed.flatMap(fd => timer.update(fd._1))
      }
      run.use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("bipartite.time.count") {
    print("bipartite.time.count: ")
    var i: Int = 0
    service.eventStream { ag =>
      ag.action("t", _.bipartite.counted.timed).retry(IO(i += 1)).buildWith(identity).use {
        _.run(()).foreverM.timeout(take).attempt
      }
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("bipartite.time") {
    print("bipartite.time: ")
    var i = 0
    service.eventStream { ag =>
      ag.action("t", _.bipartite.timed).retry(IO(i += 1)).buildWith(identity).use {
        _.run(()).foreverM.timeout(take).attempt
      }
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }
  test("kleisli.bipartite.time") {
    print("kleisli.bipartite.time: ")
    var i = 0
    service.eventStream { ga =>
      val run = for {
        action <- ga.action("t", _.bipartite).retry(IO(i += 1).timed).buildWith(identity)
        timer <- ga.timer("t")
      } yield for {
        (t, _) <- action
        _ <- timer.kleisli((_: Unit) => t)
      } yield ()
      run.use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("bipartite.counting") {
    print("bipartite.count: ")
    var i = 0
    service.eventStream { ag =>
      ag.action("t", _.bipartite.counted)
        .retry(IO(i += 1))
        .buildWith(identity)
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("bipartite") {
    var i = 0
    print("bipartite: ")
    service.eventStream { ag =>
      ag.action("t", _.bipartite)
        .retry(IO(i += 1))
        .buildWith(identity)
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("unipartite.time.count") {
    print("unipartite.time.count: ")
    var i: Int = 0
    service.eventStream { ag =>
      ag.action("t", _.unipartite.counted.timed)
        .retry(IO(i += 1))
        .buildWith(identity)
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("unipartite.time") {
    print("unipartite.time: ")
    var i: Int = 0
    service.eventStream { ag =>
      ag.action("t", _.unipartite.timed)
        .retry(IO(i += 1))
        .buildWith(identity)
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }
  test("kleisli.unipartite.time") {
    print("kleisli.unipartite.time: ")
    var i = 0
    service.eventStream { ga =>
      val run = for {
        action <- ga.action("t", _.unipartite).retry(IO(i += 1).timed).buildWith(identity)
        timer <- ga.timer("t")
      } yield for {
        (t, _) <- action
        _ <- timer.kleisli((_: Unit) => t)
      } yield ()
      run.use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("unipartite.counting") {
    print("unipartite.count: ")
    var i: Int = 0
    service.eventStream { ag =>
      ag.action("t", _.unipartite.counted)
        .retry(IO(i += 1))
        .buildWith(identity)
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("unipartite") {
    print("unipartite: ")
    var i: Int = 0
    service.eventStream { ag =>
      ag.action("t", _.unipartite)
        .retry(IO(i += 1))
        .buildWith(identity)
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("silent.time.count") {
    print("silent.time.count: ")
    var i = 0
    service.eventStream { ag =>
      ag.action("t", _.silent.timed.counted)
        .retry(IO(i += 1))
        .buildWith(identity)
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("silent.time") {
    print("silent.time: ")
    var i: Int = 0
    service.eventStream { ag =>
      ag.action("t", _.silent.timed)
        .retry(IO(i += 1))
        .buildWith(identity)
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }
  test("kleisli.silent.time") {
    print("kleisli.silent.time: ")
    var i = 0
    service.eventStream { ga =>
      val run = for {
        action <- ga.action("t", _.silent).retry(IO(i += 1).timed).buildWith(identity)
        timer <- ga.timer("t")
      } yield for {
        (t, _) <- action
        _ <- timer.kleisli((_: Unit) => t)
      } yield ()
      run.use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("silent.counting") {
    print("silent.count: ")
    var i: Int = 0
    service.eventStream { ag =>
      ag.action("t", _.silent.counted)
        .retry(IO(i += 1))
        .buildWith(identity)
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("silent") {
    print("silent: ")
    var i: Int = 0
    service.eventStream { ag =>
      ag.action("t", _.silent)
        .retry(IO(i += 1))
        .buildWith(identity)
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("flow-meter") {
    print("flow-meter: ")
    var i: Int = 0
    service.eventStream { ag =>
      val name = "flow-meter"
      ag.flowMeter(name, _.withUnit(_.COUNT)).use(_.update(1).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("meter") {
    var i = 0
    print("meter: ")
    service.eventStream { ag =>
      ag.meter("meter").use(_.mark(1).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("meter.count") {
    var i = 0
    print("meter.count: ")
    service.eventStream { ag =>
      ag.meter("meter", _.counted).use(_.mark(1).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("histogram") {
    var i = 0
    print("histogram: ")
    service.eventStream { ag =>
      ag.histogram("histogram").use(_.update(1).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("histogram.count") {
    var i = 0
    print("histogram.count: ")
    service.eventStream { ag =>
      ag.histogram("histogram", _.counted).use(_.update(1).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("timer") {
    var i = 0
    print("timer: ")
    service.eventStream { ag =>
      ag.timer("timer").use(_.update(1.seconds).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("timer.count") {
    var i = 0
    print("timer.count: ")
    service.eventStream { ag =>
      ag.timer("timer", _.counted).use(_.update(1.seconds).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("count") {
    var i = 0
    print("count: ")
    service.eventStream { ag =>
      ag.counter("count").use(_.inc(1).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }
}
