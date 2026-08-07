package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class AwsPackageHelpersTest extends AnyFunSuite {

  private val logger: Logger[IO] = Slf4jLogger.create[IO].unsafeRunSync()

  test("shutdown completes even when close throws") {
    val result = shutdown[IO]("test-service", logger)(throw new IllegalStateException("boom")).unsafeRunSync()
    assert(result == ())
  }

  test("blockingF returns a successful value") {
    val result = blockingF[IO, Int](42, "test-context", logger).unsafeRunSync()
    assert(result == 42)
  }

  test("blockingF propagates the original exception") {
    val ex = intercept[IllegalStateException] {
      blockingF[IO, Int](throw new IllegalStateException("boom"), "test-context", logger).unsafeRunSync()
    }
    assert(ex.getMessage == "boom")
  }
}
