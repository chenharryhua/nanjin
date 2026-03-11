package mtest.common

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, TickedValue}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt
import io.circe.syntax.given
class PolicyTest extends AnyFunSuite {

  test("policy") {
    val policy =
      Policy.crontab(_.every5Minutes).jitter(30.seconds)

    tickStream.testPolicy[IO]((_: Policy.type) => policy)
      .take(3)
      .map(t => TickedValue(t, 1))
      .map(_.asJson)
      .debug()
      .compile
      .toList.unsafeRunSync()
  }
}
