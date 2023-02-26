package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{
  ActionComplete,
  ActionStart,
  PassThrough,
  ServiceStart,
  ServiceStop
}
import eu.timepit.refined.auto.*
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies

import java.time.{Duration, Instant, ZoneId}
import scala.concurrent.duration.{DurationDouble, DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.JavaDurationOps

class TicksTest extends AnyFunSuite {
  val service = TaskGuard[IO]("awake").service("every")
  test("1. should not lock - even") {
    val List(a, b, c, d) = service
      .eventStream(agent =>
        agent
          .ticks(cron_1second)
          .evalMap(idx => agent.action("even", _.notice).retry(IO(idx)).run)
          .compile
          .drain)
      .take(4)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionComplete])
    assert(d.isInstanceOf[ActionStart])
  }

  test("2. should not lock - odd") {
    val List(a, b, c, d, e) = service
      .eventStream(agent =>
        agent
          .ticks(cron_1second)
          .evalMap(idx => agent.action("odd", _.notice).retry(IO(idx)).run)
          .compile
          .drain)
      .take(5)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionComplete])
    assert(d.isInstanceOf[ActionStart])
    assert(e.isInstanceOf[ActionComplete])
  }

  test("3. policy based awakeEvery") {
    val List(a, b, c, d, e, f, g, h) =
      service
        .eventStream(agent =>
          agent
            .ticks(cron_1second, _.join(RetryPolicies.limitRetries(3)))
            .evalMap(idx => agent.action("policy", _.notice).retry(IO(idx)).run)
            .compile
            .drain)
        .compile
        .toList
        .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionComplete])
    assert(d.isInstanceOf[ActionStart])
    assert(e.isInstanceOf[ActionComplete])
    assert(f.isInstanceOf[ActionStart])
    assert(g.isInstanceOf[ActionComplete])
    assert(h.isInstanceOf[ServiceStop])
  }

  test("4.cron index") {

    val lst = service
      .eventStream(ag =>
        ag.ticks(cron_1minute, RetryPolicies.capDelay[IO](1.second, _))
          .evalMap(x => ag.broker("pt").passThrough(x.asJson))
          .take(3)
          .compile
          .drain)
      .compile
      .toList
      .map(_.filter(_.isInstanceOf[PassThrough]))
      .unsafeRunSync()
    assert(List(0, 1, 2) == lst.flatMap(_.asInstanceOf[PassThrough].value.asNumber.flatMap(_.toLong)))
  }

  test("5. fib awakeEvery") {
    val policy = RetryPolicies.fibonacciBackoff[IO](1.second).join(RetryPolicies.limitRetries(3))
    val List(a, b, c, d, e, f, g, h) =
      service
        .eventStream(agent =>
          agent.ticks(policy).evalMap(idx => agent.action("fib", _.notice).retry(IO(idx)).run).compile.drain)
        .compile
        .toList
        .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionComplete])
    assert(d.isInstanceOf[ActionStart])
    assert(e.isInstanceOf[ActionComplete])
    assert(f.isInstanceOf[ActionStart])
    assert(g.isInstanceOf[ActionComplete])
    assert(h.isInstanceOf[ServiceStop])
  }

  test("6.tick ") {
    val policy = policies.cronBackoff[IO](cron_1second, ZoneId.systemDefault())
    val ticks  = awakeEvery(policy)

    ticks
      .merge(ticks)
      .merge(ticks)
      .merge(ticks)
      .merge(ticks)
      .evalMap(idx => IO.realTimeInstant.map((_, idx)))
      .take(20)
      .fold(Map.empty[Int, List[Instant]]) { case (sum, (fd, idx)) =>
        sum.updatedWith(idx)(ls => Some(fd :: ls.sequence.flatten))
      }
      .map { m =>
        assert(m.forall(_._2.size == 5)) // 5 streams
        // less than 0.1 second for the same index
        m.foreach { case (_, ls) =>
          ls.zip(ls.reverse).map { case (a, b) =>
            val dur = Duration.between(a, b).abs().toScala
            assert(dur < 0.1.seconds)
          }
        }

        m.flatMap(_._2.headOption).toList.sorted.sliding(2).map { ls =>
          val diff = Duration.between(ls(1), ls(0)).abs.toScala
          assert(diff > 0.9.second && diff < 1.1.seconds)
        }
      }
      .compile
      .drain
      .unsafeRunSync()
  }

  test("7.process longer than 1 second") {
    val policy = policies.cronBackoff[IO](cron_1second, ZoneId.systemDefault())
    val ticks  = awakeEvery(policy)

    val fds: List[FiniteDuration] =
      ticks.evalMap(_ => IO.sleep(1.5.seconds) >> IO.monotonic).take(5).compile.toList.unsafeRunSync()

    fds.sliding(2).foreach {
      case List(a, b) =>
        val diff = b - a
        assert(diff > 1.9.seconds && diff < 2.2.seconds)
      case _ => throw new Exception("not happen")
    }
  }

  test("8.process less than 1 second") {
    val policy = policies.cronBackoff[IO](cron_1second, ZoneId.systemDefault())
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
}
