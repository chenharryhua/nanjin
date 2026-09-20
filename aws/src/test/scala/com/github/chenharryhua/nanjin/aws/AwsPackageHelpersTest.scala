package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import munit.CatsEffectSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class AwsPackageHelpersTest extends CatsEffectSuite {

  private val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  test("shutdown completes even when close throws") {
    shutdown[IO]("test-service", logger)(throw new IllegalStateException("boom")).map { result =>
      assert(result == ())
    }
  }

  test("blockingF returns a successful value") {
    blockingF[IO, Int](42, "test-context", logger).map { result =>
      assert(result == 42)
    }
  }

  test("blockingF propagates the original exception") {
    interceptIO[IllegalStateException] {
      blockingF[IO, Int](throw new IllegalStateException("boom"), "test-context", logger)
    }.map { ex =>
      assert(ex.getMessage == "boom")
    }
  }
}
