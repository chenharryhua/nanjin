package mtest.common
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Temporal}
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick}
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

import java.time.{Duration as JDuration, LocalTime, ZoneId}
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.*
class TickStreamSpec extends AnyFunSuite {

  val zoneId: ZoneId = ZoneId.systemDefault()
  val policy: Policy = Policy.fixedDelay(100.milliseconds) // example fixed policy

  private def takeTicks[F[_]: Temporal](stream: Stream[F, Tick], n: Long): F[List[Tick]] =
    stream.take(n).compile.toList

  test("1.tickImmediate emits zeroth tick immediately") {
    val ticks = takeTicks(tickStream.tickImmediate[IO](zoneId, policy), 3).unsafeRunSync()
    assert(ticks.nonEmpty)
    assert(ticks.head.index == 0)
    assert(ticks.sliding(2).forall {
      case Seq(a, b) => a.conclude.isBefore(b.conclude) || a.conclude.equals(b.conclude)
      case _         => true
    })
  }

  test("2.tickScheduled emits first tick after snooze") {
    val ticks = takeTicks(tickStream.tickScheduled[IO](zoneId, policy), 3).unsafeRunSync()
    assert(ticks.nonEmpty)
    assert(ticks.head.index == 1)
    assert(ticks.sliding(2).forall {
      case Seq(a, b) => a.conclude.isBefore(b.conclude) || a.conclude.equals(b.conclude)
      case _         => true
    })
  }

  test("3.tickFuture emits first tick immediately and sleeps afterward") {
    val start = LocalTime.now()
    val ticks = takeTicks(tickStream.tickFuture[IO](zoneId, Policy.fixedDelay(2.seconds)), 3).unsafeRunSync()
    val elapsed = JDuration.between(start, LocalTime.now())
    assert(ticks.nonEmpty)
    assert(ticks.head.index == 1)
    val expectedMinDuration = ticks.dropRight(1).map(_.snooze.toScala).foldLeft(0.seconds)(_ + _)
    assert(elapsed.toScala >= expectedMinDuration)
    assert(JDuration.between(start, ticks.head.local(_.acquires).toLocalTime).toScala < 1.second)
  }

}
