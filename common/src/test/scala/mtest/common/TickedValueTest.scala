package mtest.common

import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickedValue}
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.{Duration, Instant, ZoneId}
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

class TickedValueTest extends AnyFunSuite with Matchers {

  test("TickedValue map preserves tick") {
    val tick = Tick.zeroth(UUID.randomUUID(), ZoneId.of("UTC"), Instant.now())
    val tv = TickedValue(tick, 10)
    val mapped = tv.map(_ * 2)

    mapped.value shouldEqual 20
    mapped.tick shouldEqual tick
  }

  test("TickedValue withSnoozeStretch updates tick") {
    val tick = Tick.zeroth(UUID.randomUUID(), ZoneId.of("UTC"), Instant.now())
    val tv = TickedValue(tick, "x")
    val updated = tv.withSnoozeStretch(Duration.ofSeconds(5))

    updated.tick.conclude shouldEqual tick.conclude.plusSeconds(5)
  }

  test("TickedValue withConclude updates tick") {
    val tick = Tick.zeroth(UUID.randomUUID(), ZoneId.of("UTC"), Instant.now())
    val tv = TickedValue(tick, "x")
    val newConclude = tick.conclude.plusSeconds(100)
    val updated = tv.withConclude(newConclude)

    updated.tick.conclude shouldEqual newConclude
  }

  test("TickedValue resolveTime produces TimeStamped") {
    val tick = Tick.zeroth(UUID.randomUUID(), ZoneId.of("UTC"), Instant.now())
    val tv = TickedValue(tick, 42)
    val ts = tv.resolveTime(t => FiniteDuration(t.active.toMillis, scala.concurrent.duration.MILLISECONDS))

    ts.value shouldEqual 42
  }

  test("TickedValue JSON encoding and decoding") {
    val tick = Tick.zeroth(UUID.randomUUID(), ZoneId.of("UTC"), Instant.now())
    val tv = TickedValue(tick, 123)
    val json = tv.asJson.noSpaces
    val decoded = decode[TickedValue[Int]](json).toOption.get

    decoded.value shouldEqual tv.value
    decoded.tick shouldEqual tv.tick
  }

  test("TickedValue Functor instance map works") {
    val tick = Tick.zeroth(UUID.randomUUID(), ZoneId.of("UTC"), Instant.now())
    val tv = TickedValue(tick, 10)
    val mapped = TickedValue.functorTickedValue.map(tv)(_ + 5)
    mapped.value shouldEqual 15
    mapped.tick shouldEqual tv.tick
  }
}
