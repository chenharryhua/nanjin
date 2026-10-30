package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
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

  test("1.containerMetadata yields None off-ECS without issuing a request") {
    assume(
      sys.env.get("ECS_CONTAINER_METADATA_URI_V4").isEmpty &&
        sys.env.get("ECS_CONTAINER_METADATA_URI").isEmpty,
      "test host appears to be running on ECS")

    ecs.containerMetadata[IO].map(result => assertEquals(result, None))
  }

  test("2.containerMetadataTask yields None off-ECS without issuing a request") {
    assume(
      sys.env.get("ECS_CONTAINER_METADATA_URI_V4").isEmpty &&
        sys.env.get("ECS_CONTAINER_METADATA_URI").isEmpty,
      "test host appears to be running on ECS")

    ecs.containerMetadataTask[IO].map(result => assertEquals(result, None))
  }
}
