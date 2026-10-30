package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import cats.syntax.apply.given
import cats.syntax.traverse.given
import munit.CatsEffectSuite

import scala.concurrent.duration.DurationInt

/** Off-ECS coverage for `CloudWatchLogLocator.apply`.
  *
  * Running off-ECS (no container-metadata env var), `ecs.containerMetadata` yields `None`, so `apply` takes
  * its warning branch and returns `None` with no network access. The private pure link-building (`parse`,
  * `encode`, `fromMetadata`, and `locate`) is only reachable through a non-`None` metadata document, which
  * requires a live ECS metadata endpoint. Reaching it would need a production seam this coverage pass does
  * not add, so that path is intentionally left uncovered.
  */
class CloudWatchLogLocatorTest extends CatsEffectSuite {

  private val offEcs: Boolean =
    sys.env.get("ECS_CONTAINER_METADATA_URI_V4").isEmpty &&
      sys.env.get("ECS_CONTAINER_METADATA_URI").isEmpty

  test("1.apply yields None off-ECS when container metadata is unavailable") {
    assume(offEcs, "test host appears to be running on ECS")

    CloudWatchLogLocator[IO](5.minutes).map(locator => assertEquals(locator, None))
  }

  test("2.apply is total off-ECS across window values and never throws") {
    assume(offEcs, "test host appears to be running on ECS")

    val windows = List(0.seconds, 1.second, 30.seconds, 5.minutes, 24.hours)
    windows.traverse(w => CloudWatchLogLocator[IO](w)).map { locators =>
      assertEquals(locators, List.fill(windows.size)(None))
    }
  }

  test("3.apply off-ECS is repeatable and side-effect free") {
    assume(offEcs, "test host appears to be running on ECS")

    val locator = CloudWatchLogLocator[IO](5.minutes)
    (locator, locator).tupled.map { case (first, second) =>
      assertEquals(first, None)
      assertEquals(second, None)
    }
  }
}
