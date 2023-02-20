package mtest.guard

import cats.effect.IO
import cats.effect.std.AtomicCell
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import eu.timepit.refined.auto.*
import fs2.concurrent.SignallingRef
import io.circe.Json
import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

// sbt "guard/testOnly mtest.guard.PerformanceTest"

/** last time: (run more than once, pick up the best)
  *
  * 317k/s critical
  *
  * 606k/s silent
  *
  * 281k/s critical with notes
  *
  * 510k/s trivial with Timing and Counting
  *
  * 264k/s notice with Timing and Counting
  *
  * atomicBox vs cats.atomicCell
  *
  * 653k/s cats.atomicCell
  *
  * 588k/s atomicBox signalBox vs fs2.SignallingRef
  *
  * 2174k/s signalBox
  *
  * 4183k/s fs2.SignallingRef
  */

@Ignore
class PerformanceTest extends AnyFunSuite {
  val service: ServiceGuard[IO] =
    TaskGuard[IO]("performance").service("actions").updateConfig(_.withMetricReport(cron_1second))
  val take: FiniteDuration = 100.seconds

  def speed(i: Int): String = f"${i / (take.toSeconds * 1000)}%4dk/s"

  ignore("alert") {
    var i = 0
    service.eventStream { ag =>
      val ts = ag.alert("alert").info("alert").map(_ => i += 1)
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} alert")
  }

  test("with trace") {
    var i = 0
    service.eventStream { ag =>
      val ts = ag.action("trace").retry(IO(i += 1))
      ag.root("root").use(s => ts.runWithSpan(s).foreverM.timeout(take)).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} trace")
  }

  test("atomicBox vs cats.atomicCell") {
    service.eventStream { agent =>
      val box = agent.atomicBox(IO(0))
      val cats = AtomicCell[IO]
        .of[Int](0)
        .flatMap(r =>
          r.update(_ + 1).foreverM.timeout(take).attempt >> r.get.flatMap(c =>
            IO.println(s"${speed(c)} cats.atomicCell")))

      val nj =
        box.update(_ + 1).foreverM.timeout(take).attempt >> box.get.flatMap(c =>
          IO.println(s"${speed(c)} atomicBox"))

      IO.println("atomicBox vs cats.atomicCell") >> (nj &> cats)
    }.compile.drain.unsafeRunSync()
  }

  test("signalBox vs fs2.SignallingRef") {
    service.eventStream { agent =>
      val box = agent.signalBox(0)
      val cats = SignallingRef[IO]
        .of[Int](0)
        .flatMap(r =>
          r.update(_ + 1).foreverM.timeout(take).attempt >> r.get.flatMap(c =>
            IO.println(s"${speed(c)} fs2.SignallingRef")))

      val nj =
        box.update(_ + 1).foreverM.timeout(take).attempt >> box.get.flatMap(c =>
          IO.println(s"${speed(c)} signalBox"))

      IO.println("signalBox vs fs2.SignallingRef") >> (nj &> cats)
    }.compile.drain.unsafeRunSync()
  }

  test("critical") {
    var i = 0
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.critical.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} critical")
  }

  test("silent") {
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.silent.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} silent")
  }

  test("critical with notes") {
    var i = 0
    service.eventStream { ag =>
      val ts = ag
        .action("t", _.critical.withoutTiming.withoutCounting)
        .retry((_: Int) => IO(i += 1))
        .logOutput((i, _) => Json.fromInt(i))

      ts.run(1).foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} critical with notes")
  }

  test("trivial with Timing and Counting") {
    var i = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.trivial.withTiming.withCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} trivial with Timing and Counting")
  }

  test("notice notice with Timing and Counting") {
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.notice.withTiming.withCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} notice with Timing and Counting")
  }

}
