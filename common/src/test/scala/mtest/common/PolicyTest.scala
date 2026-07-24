package mtest.common

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, TickedValue}
import io.circe.jawn.decode
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt
import io.circe.syntax.given
class PolicyTest extends AnyFunSuite {

  test("1.policy") {
    val policy =
      Policy.crontab(_.every5Minutes).jitter(30.seconds)

    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    val ticks = tickStream.testPolicy[IO]((_: Policy.type) => policy).take(3).compile.toList.unsafeRunSync()

    assert(ticks.size == 3)
    assert(ticks.map(_.index) == List(1L, 2L, 3L))
    assert(ticks.map(_.sequenceId).distinct.size == 1)

    ticks.foreach { tick =>
      val decoded = decode[TickedValue[Int]](TickedValue(tick, 1).asJson.noSpaces).toOption.get
      assert(decoded.value == 1)
    }
  }
}
