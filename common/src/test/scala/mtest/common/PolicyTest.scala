package mtest.common

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, TickedValue}
import io.circe.jawn.decode
import munit.CatsEffectSuite

import scala.concurrent.duration.DurationInt
import io.circe.syntax.given
class PolicyTest extends CatsEffectSuite {

  test("1.policy") {
    val policy =
      Policy.crontab(_.every5Minutes).repeat.jitter(30.seconds)

    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    tickStream.testPolicy[IO]((_: Policy.type) => policy).take(3).compile.toList.map { ticks =>
      assert(ticks.size == 3)
      assert(ticks.map(_.index) == List(1L, 2L, 3L))
      assert(ticks.map(_.sequenceId).distinct.size == 1)

      ticks.foreach { tick =>
        val decoded = decode[TickedValue[Int]](TickedValue(tick, 1).asJson.noSpaces).toOption.get
        assert(decoded.value == 1)
      }
    }
  }

  test("2.fixedRate wire format is a single duration under the fixedRate key") {
    val policy = Policy.fixedRate(5.seconds)
    val json = policy.asJson
    // the encoder emits one scalar Duration (ISO-8601 "PT5S"), not an array; this guards the wire format
    assert(json.hcursor.get[String]("fixedRate").toOption.contains("PT5S"))
    // and it still round-trips
    assert(decode[Policy](json.noSpaces).toOption.contains(policy))
  }
}
