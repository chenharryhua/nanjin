package mtest.common

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.{Policy, PolicyTick}
import munit.CatsEffectSuite

import java.time.ZoneId
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
}
