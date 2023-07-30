package mtest

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.datetime.zones.sydneyTime
import com.github.chenharryhua.nanjin.datetime.{Tick, awakeEvery, crontabs, policies}
import org.scalatest.funsuite.AnyFunSuite

import java.time.{Duration, Instant, ZoneId}
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.JavaDurationOps

class AwakeEveryTest extends AnyFunSuite {
  test("1.tick") {
    val policy = policies.cronBackoff[IO](crontabs.secondly, ZoneId.systemDefault())
    val ticks  = awakeEvery(policy)

    ticks
      .merge(ticks)
      .merge(ticks)
      .merge(ticks)
      .merge(ticks)
      .evalMap(idx => IO.realTimeInstant.map((_, idx)))
      .take(20)
      .fold(Map.empty[Int, List[Instant]]) { case (sum, (fd, idx)) =>
        sum.updatedWith(idx.index)(ls => Some(fd :: ls.sequence.flatten))
      }
      .map { m =>
        assert(m.forall(_._2.size == 5)) // 5 streams
        // less than 0.1 second for the same index
        m.foreach { case (_, ls) =>
          ls.zip(ls.reverse).map { case (a, b) =>
            val dur = Duration.between(a, b).abs().toScala
            assert(dur < 0.2.seconds)
          }
        }

        m.flatMap(_._2.headOption).toList.sorted.sliding(2).map { ls =>
          val diff = Duration.between(ls(1), ls.head).abs.toScala
          assert(diff > 0.9.second && diff < 1.1.seconds)
        }
      }
      .compile
      .drain
      .unsafeRunSync()
  }

  test("2.process longer than 1 second") {
    val policy = policies.cronBackoff[IO](crontabs.secondly, ZoneId.systemDefault())
    val ticks  = awakeEvery(policy)

    val fds: List[FiniteDuration] =
      ticks.evalMap(_ => IO.sleep(1.5.seconds) >> IO.monotonic).take(5).compile.toList.unsafeRunSync()

    fds.sliding(2).foreach {
      case List(a, b) =>
        val diff = b - a
        assert(diff > 1.9.seconds && diff < 2.1.seconds)
      case _ => throw new Exception("not happen")
    }
  }

  test("3.process less than 1 second") {
    val policy = policies.cronBackoff[IO](crontabs.secondly, sydneyTime)
    val ticks  = awakeEvery(policy)

    val fds: List[FiniteDuration] =
      ticks.evalMap(_ => IO.sleep(0.5.seconds) >> IO.monotonic).take(5).compile.toList.unsafeRunSync()
    fds.sliding(2).foreach {
      case List(a, b) =>
        val diff = b - a
        assert(diff > 0.9.seconds && diff < 1.1.seconds)
      case _ => throw new Exception("not happen")
    }
  }

  test("4.ticks - cron") {
    val policy = policies.limitRetriesByCumulativeDelay[IO](
      5.seconds,
      policies.cronBackoff[IO](crontabs.secondly, ZoneId.systemDefault()))
    val ticks = awakeEvery(policy).debug().compile.toList.unsafeRunSync()
    assert(ticks.size === 5)
    val spend = Duration.between(ticks.head.wakeTime, ticks(4).wakeTime)
    assert(spend.toSeconds === 4)
  }

  test("7. limitRetriesByDelay") {
    val policy =
      policies.limitRetriesByDelay[IO](1.seconds, policies.cronBackoff[IO](crontabs.hourly, sydneyTime))
    val res: List[Tick] = awakeEvery(policy).compile.toList.unsafeRunSync()
    assert(res.isEmpty)
  }
}
