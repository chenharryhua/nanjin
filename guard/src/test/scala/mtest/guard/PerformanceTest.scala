package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.funsuite.AnyFunSuite

import java.text.DecimalFormat
import scala.concurrent.duration.*

/** ---warm-up: 424K/s, 2 micro 355 nano
  *
  * bipartite.time.count: 417K/s, 2 micro 397 nano
  *
  * bipartite.time: 449K/s, 2 micro 225 nano
  *
  * bipartite.count: 514K/s, 1 micro 943 nano
  *
  * bipartite: 523K/s, 1 micro 910 nano
  *
  * ---kleisli.bipartite.time: 339K/s, 2 micro 947 nano
  *
  * unipartite.time.count: 634K/s, 1 micro 577 nano
  *
  * unipartite.time: 635K/s, 1 micro 573 nano
  *
  * unipartite.count: 759K/s, 1 micro 316 nano
  *
  * unipartite: 769K/s, 1 micro 298 nano
  *
  * ---kleisli.unipartite.time: 420K/s, 2 micro 377 nano
  *
  * silent.time.count: 1543K/s, 648 nano
  *
  * silent.time: 1548K/s, 645 nano
  *
  * silent.count: 4428K/s, 225 nano
  *
  * silent: 4595K/s, 217 nano
  *
  * ---kleisli.silent.time: 1000K/s, 999 nano
  *
  * flow-meter: 3329K/s, 300 nano
  *
  * meter: 5751K/s, 173 nano
  *
  * meter.count: 4391K/s, 227 nano
  *
  * histogram: 4069K/s, 245 nano
  *
  * histogram.count: 3436K/s, 290 nano
  *
  * timer: 3245K/s, 308 nano
  *
  * timer.count: 2794K/s, 357 nano
  *
  * count: 5952K/s, 168 nano
  */

class PerformanceTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] =
    TaskGuard[IO]("performance")
      .service("actions")
      .updateConfig(_.withMetricReport(Policy.crontab(_.secondly)))

  // sbt "guard/testOnly mtest.guard.PerformanceTest"
  private val take: FiniteDuration = 5.seconds

  private def speed(i: Int): String = {
    val df = new DecimalFormat("#,###.##")
    s"\t${df.format(i / take.toSeconds)} /s, \t${fmt.format(take / i.toLong)}"
  }

  test("baseline") {
    print("---baseline: ")
    var i = 0
    service.eventStream(_ => IO(i += 1).foreverM.timeout(take).attempt).compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("disabled") {
    print("---disabled: ")
    var i: Int = 0
    service.eventStream { ag =>
      ag.action("t", _.bipartite.counted.timed.enable(false)).retry(IO(i += 1)).buildWith(identity).use {
        _.run(()).foreverM.timeout(take).attempt
      }
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
    print("bipartite.time:     ")
    var i = 0
    service.eventStream { ag =>
      ag.action("t", _.bipartite.timed).retry(IO(i += 1)).buildWith(identity).use {
        _.run(()).foreverM.timeout(take).attempt
      }
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("bipartite.counting") {
    print("bipartite.count:    ")
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
    print("bipartite:        ")
    service.eventStream { ag =>
      ag.action("t", _.bipartite)
        .retry(IO(i += 1))
        .buildWith(identity)
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("kleisli.bipartite.time") {
    print("---kleisli.bipartite.time: ")
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
    print("unipartite.time:      ")
    var i: Int = 0
    service.eventStream { ag =>
      ag.action("t", _.unipartite.timed)
        .retry(IO(i += 1))
        .buildWith(identity)
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("unipartite.counting") {
    print("unipartite.count:    ")
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
    print("unipartite:         ")
    var i: Int = 0
    service.eventStream { ag =>
      ag.action("t", _.unipartite)
        .retry(IO(i += 1))
        .buildWith(identity)
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("kleisli.unipartite.time") {
    print("---kleisli.unipartite.time: ")
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
    print("silent.time:      ")
    var i: Int = 0
    service.eventStream { ag =>
      ag.action("t", _.silent.timed)
        .retry(IO(i += 1))
        .buildWith(identity)
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("silent.counting") {
    print("silent.count:     ")
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
    print("silent:           ")
    var i: Int = 0
    service.eventStream { ag =>
      ag.action("t", _.silent)
        .retry(IO(i += 1))
        .buildWith(identity)
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("kleisli.silent.time") {
    print("---kleisli.silent.time: ")
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

  test("flow-meter") {
    print("flow-meter:       ")
    var i: Int = 0
    service.eventStream { ag =>
      val name = "flow-meter"
      ag.flowMeter(name, _.withUnit(_.COUNT)).use(_.update(1).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("meter") {
    var i = 0
    print("meter:            ")
    service.eventStream { ag =>
      ag.meter("meter").use(_.update(1).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("meter.count") {
    var i = 0
    print("meter.count:      ")
    service.eventStream { ag =>
      ag.meter("meter", _.counted).use(_.update(1).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("histogram") {
    var i = 0
    print("histogram:        ")
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
    print("timer:          ")
    service.eventStream { ag =>
      ag.timer("timer").use(_.update(1.seconds).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("timer.count") {
    var i = 0
    print("timer.count:    ")
    service.eventStream { ag =>
      ag.timer("timer", _.counted).use(_.update(1.seconds).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("count") {
    var i = 0
    print("count:          ")
    service.eventStream { ag =>
      ag.counter("count").use(_.inc(1).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }
}
