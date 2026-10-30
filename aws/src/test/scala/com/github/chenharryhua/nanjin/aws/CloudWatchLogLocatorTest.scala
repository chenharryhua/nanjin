package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
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

  test("1.apply yields None off-ECS when container metadata is unavailable") {
    assume(
      sys.env.get("ECS_CONTAINER_METADATA_URI_V4").isEmpty &&
        sys.env.get("ECS_CONTAINER_METADATA_URI").isEmpty,
      "test host appears to be running on ECS")

    CloudWatchLogLocator[IO](5.minutes).map(locator => assertEquals(locator, None))
  }
}
