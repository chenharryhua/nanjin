package mtest.common

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.{Policy, PolicyTick}
import munit.CatsEffectSuite

import java.time.{Duration as JavaDuration, ZoneId}
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps

class PolicyTickTest extends CatsEffectSuite {

  private val zoneId = ZoneId.of("UTC")
  private val delay = 5.seconds

  // seed.tick.conclude == seed instant, so `next(now)` straddles tick.conclude depending on `now`.

  test("1.next steps from tick.conclude when now is before conclude") {
    for {
      seed <- PolicyTick.seed[IO](zoneId, Policy.fixedDelay(delay))
      before = seed.tick.conclude.minusSeconds(1)
      stepped <- seed.next(before)
    } yield {
      val next = stepped.get
      // the true arm uses tick.conclude as the step base, so acquires lands on the old conclude
      assertEquals(next.tick.acquires, seed.tick.conclude)
      assertEquals(next.tick.conclude, seed.tick.conclude.plus(delay.toJava))
    }
  }

  test("2.next steps from now when now is on or after conclude") {
    for {
      seed <- PolicyTick.seed[IO](zoneId, Policy.fixedDelay(delay))
      after = seed.tick.conclude.plusSeconds(1)
      stepped <- seed.next(after)
    } yield {
      val next = stepped.get
      // the false arm uses `now` as the step base
      assertEquals(next.tick.acquires, after)
      assertEquals(next.tick.conclude, after.plus(delay.toJava))
    }
  }

  test("3.renewPolicy swaps the steppers while keeping the current tick") {
    for {
      seed <- PolicyTick.seed[IO](zoneId, Policy.empty)
      renewed = seed.renewPolicy(Policy.fixedDelay(delay))
      after = renewed.tick.conclude.plusSeconds(1)
      stepped <- renewed.next(after)
    } yield {
      assertEquals(renewed.tick, seed.tick)
      assertEquals(stepped.get.tick.conclude, after.plus(delay.toJava))
    }
  }

  test("4.fixedRate catches up across a long gap in a single step") {
    // The catch-up arm runs from sequence index 1 onwards. It must land on the first cadence boundary
    // strictly after `now`, however many periods were missed, and stay aligned to the previous conclude
    // rather than restarting from `now`. A 10 hour gap at a one second rate is 36,000 missed periods,
    // which the previous implementation walked one at a time.
    val rate = 1.second
    val gap = 10.hours

    for {
      seed <- PolicyTick.seed[IO](zoneId, Policy.fixedRate(rate).repeat)
      first <- seed.next(seed.tick.conclude)
      anchor = first.get.tick.conclude
      now = anchor.plus(gap.toJava)
      caught <- first.get.next(now)
    } yield {
      val conclude = caught.get.tick.conclude
      assert(conclude.isAfter(now), s"$conclude must be strictly after $now")
      // lands on the very next boundary, so nothing was skipped past
      val overshoot = JavaDuration.between(now, conclude)
      assert(overshoot.compareTo(rate.toJava) <= 0, s"overshoot $overshoot exceeds one period")
      // and remains on the original cadence: a whole number of periods from the anchor
      val fromAnchor = JavaDuration.between(anchor, conclude)
      assertEquals(fromAnchor.toNanos % rate.toJava.toNanos, 0L)
      assertEquals(fromAnchor.toNanos / rate.toJava.toNanos, gap.toSeconds + 1)
    }
  }
}
