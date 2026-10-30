package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import cats.syntax.apply.given
import munit.CatsEffectSuite

/** Off-ECS coverage for `ecs.containerMetadata`.
  *
  * When neither `ECS_CONTAINER_METADATA_URI_V4` nor `ECS_CONTAINER_METADATA_URI` is set — the case for any
  * host that is not an ECS task — `metaUri` yields `None`, so no HTTP request is ever made and the result is
  * `None`. These tests assume the test JVM runs off-ECS, which holds in CI and local development. The success
  * path needs a live metadata endpoint and is intentionally left uncovered; reaching it without a real
  * endpoint would require a production seam that this coverage pass does not add.
  */
class EcsTest extends CatsEffectSuite {

  private val offEcs: Boolean =
    sys.env.get("ECS_CONTAINER_METADATA_URI_V4").isEmpty &&
      sys.env.get("ECS_CONTAINER_METADATA_URI").isEmpty

  test("1.containerMetadata yields None off-ECS without issuing a request") {
    assume(offEcs, "test host appears to be running on ECS")

    ecs.containerMetadata[IO].map(result => assertEquals(result, None))
  }

  test("2.containerMetadataTask yields None off-ECS without issuing a request") {
    assume(offEcs, "test host appears to be running on ECS")

    ecs.containerMetadataTask[IO].map(result => assertEquals(result, None))
  }

  test("3.containerMetadata off-ECS is repeatable and side-effect free") {
    assume(offEcs, "test host appears to be running on ECS")

    (ecs.containerMetadata[IO], ecs.containerMetadata[IO]).tupled.map { case (first, second) =>
      assertEquals(first, None)
      assertEquals(second, None)
    }
  }

  test("4.containerMetadataTask off-ECS is repeatable and side-effect free") {
    assume(offEcs, "test host appears to be running on ECS")

    (ecs.containerMetadataTask[IO], ecs.containerMetadataTask[IO]).tupled.map { case (first, second) =>
      assertEquals(first, None)
      assertEquals(second, None)
    }
  }
}
