package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.event.retrieveDeadlocks
import com.github.chenharryhua.nanjin.guard.observers.console
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

object deadlock {
  class Locker

  val a = new Locker
  val b = new Locker

  def m1: Int =
    a.synchronized {
      Thread.sleep(1000)
      b.synchronized {
        println("m1")
        Thread.sleep(1000)
        1
      }
    }

  def m2: Int =
    b.synchronized {
      Thread.sleep(1000)
      a.synchronized {
        println("m2")
        Thread.sleep(1000)
        1
      }
    }
}

class DeadlockTest extends AnyFunSuite {

  ignore("deadlock") {
    val locked =
      IO.interruptibleMany(deadlock.m1).timeout(3.seconds) &>
        IO.interruptibleMany(deadlock.m2).timeout(5.seconds)

    TaskGuard[IO]("deadlock")
      .service("deadlock")
      .updateConfig(_.withMetricReport(policies.crontab(_.secondly)))
      .eventStream { ga =>
        ga.jvmGauge.deadlocks.surround(locked)
      }
      .map(checkJson)
      .evalTap(console.text[IO])
      .map {
        case mr: MetricReport => retrieveDeadlocks(mr.snapshot.gauges)
        case _                => List.empty
      }
      .debug()
      .interruptAfter(15.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }
}
