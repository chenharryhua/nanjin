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
  * ---baseline: 14,704,642 /s, 68 nano
  *
  * ---disabled: 15,023,452 /s, 66 nano
  *
  * bipartite.time.count: 281,530 /s, 3 micro 552 nano
  *
  * bipartite.time: 271,775 /s, 3 micro 679 nano
  *
  * bipartite.count: 362,640 /s, 2 micro 757 nano
  *
  * bipartite: 206,261 /s, 4 micro 848 nano
  *
  * ---kleisli.bipartite.time: 234,392 /s, 4 micro 266 nano
  *
  * unipartite.time.count: 431,482 /s, 2 micro 317 nano
  *
  * unipartite.time: 442,521 /s, 2 micro 259 nano
  *
  * unipartite.count: 480,515 /s, 2 micro 81 nano
  *
  * unipartite: 500,781 /s, 1 micro 996 nano
  *
  * ---kleisli.unipartite.time: 302,935 /s, 3 micro 301 nano
  *
  * silent.time.count: 930,833 /s, 1 micro 74 nano
  *
  * silent.time: 958,693 /s, 1 micro 43 nano
  *
  * silent.count: 1,765,452 /s, 566 nano
  *
  * silent: 1,979,837 /s, 505 nano
  *
  * ---kleisli.silent.time: 706,092 /s, 1 micro 416 nano
  *
  * flow-meter: 3,027,130 /s, 330 nano
  *
  * meter: 5,177,620 /s, 193 nano
  *
  * meter.count: 4,928,916 /s, 202 nano
  *
  * histogram: 3,714,752 /s, 269 nano
  *
  * histogram.count: 3,479,467 /s, 287 nano
  *
  * timer: 2,991,335 /s, 334 nano
  *
  * timer.count: 2,951,528 /s, 338 nano
  *
  * count: 7,181,873 /s, 139 nano
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
      ag.facilitator("test", _.withPolicy(Policy.giveUp))
        .sentry
        .retry(IO(i += 1))
        .foreverM
        .timeout(take)
        .attempt
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("bipartite") {
    var i = 0
    print("bipartite:        ")
    service.eventStream { ag =>
      ag.action("t")
        .retry(IO(i += 1))
        .buildWith(_.withPublishStrategy(_.Bipartite))
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("unipartite") {
    print("unipartite:         ")
    var i: Int = 0
    service.eventStream { ag =>
      ag.action("t")
        .retry(IO(i += 1))
        .buildWith(_.withPublishStrategy(_.Unipartite))
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("silent") {
    print("silent:           ")
    var i: Int = 0
    service.eventStream { ag =>
      ag.action("t")
        .retry(IO(i += 1))
        .buildWith(_.withPublishStrategy(_.Silent))
        .use(_.run(()).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("meter") {
    var i = 0
    print("meter:            ")
    service.eventStream { ag =>
      ag.facilitator("meter")
        .metrics
        .meter("meter")
        .use(_.update(1).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("histogram") {
    var i = 0
    print("histogram:        ")
    service.eventStream { ag =>
      ag.facilitator("histogram")
        .metrics
        .histogram("histogram")
        .use(_.update(1).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("timer") {
    var i = 0
    print("timer:          ")
    service.eventStream { ag =>
      ag.facilitator("timer")
        .metrics
        .timer("timer")
        .use(_.update(1.seconds).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }

  test("count") {
    var i = 0
    print("count:          ")
    service.eventStream { ag =>
      ag.facilitator("counter")
        .metrics
        .counter("count")
        .use(_.inc(1).map(_ => i += 1).foreverM.timeout(take).attempt)
    }.compile.drain.unsafeRunSync()
    println(speed(i))
  }
}
