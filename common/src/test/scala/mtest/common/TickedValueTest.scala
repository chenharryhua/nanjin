package mtest.common

import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickedValue}
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import munit.FunSuite
import java.time.{Duration, Instant, ZoneId}
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

class TickedValueTest extends FunSuite {

  test("1.TickedValue map preserves tick") {
    val tick = Tick.seed(UUID.randomUUID(), ZoneId.of("UTC"), Instant.now())
    val tv = TickedValue(tick, 10)
    val mapped = tv.map(_ * 2)

    assertEquals(mapped.value, 20)
    assertEquals(mapped.tick, tick)
  }

  test("2.TickedValue withSnoozeStretch updates tick") {
    val tick = Tick.seed(UUID.randomUUID(), ZoneId.of("UTC"), Instant.now())
    val tv = TickedValue(tick, "x")
    val updated = tv.withSnoozeStretch(Duration.ofSeconds(5))

    assertEquals(updated.tick.conclude, tick.conclude.plusSeconds(5))
  }

  test("3.TickedValue withConclude updates tick") {
    val tick = Tick.seed(UUID.randomUUID(), ZoneId.of("UTC"), Instant.now())
    val tv = TickedValue(tick, "x")
    val newConclude = tick.conclude.plusSeconds(100)
    val updated = tv.withConclude(newConclude)

    assertEquals(updated.tick.conclude, newConclude)
  }

  test("4.TickedValue resolveTime produces TimeStamped") {
    val tick = Tick.seed(UUID.randomUUID(), ZoneId.of("UTC"), Instant.now())
    val tv = TickedValue(tick, 42)
    val ts = tv.resolveTime(t => FiniteDuration(t.active.toMillis, scala.concurrent.duration.MILLISECONDS))

    assertEquals(ts.value, 42)
  }

  test("5.TickedValue JSON encoding and decoding") {
    val tick = Tick.seed(UUID.randomUUID(), ZoneId.of("UTC"), Instant.now())
    val tv = TickedValue(tick, 123)
    val json = tv.asJson.noSpaces
    val decoded = decode[TickedValue[Int]](json).toOption.get

    assertEquals(decoded.value, tv.value)
    assertEquals(decoded.tick, tv.tick)
  }

}
