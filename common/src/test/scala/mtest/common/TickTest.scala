package mtest.common

import com.github.chenharryhua.nanjin.common.chrono.Tick
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import munit.FunSuite

import java.time.{Duration, Instant, ZoneId}
import java.util.UUID

class TickTest extends FunSuite {

  val zoneId: ZoneId = ZoneId.of("UTC")
  val now: Instant = Instant.parse("2026-02-14T00:00:00Z")
  val uuid: UUID = UUID.randomUUID()

  test("1.Tick seed should initialize correctly") {
    val tick = Tick.seed(uuid, zoneId, now)
    assertEquals(tick.sequenceId, uuid)
    assertEquals(tick.launchTime, now)
    assertEquals(tick.commence, now)
    assertEquals(tick.acquires, now)
    assertEquals(tick.conclude, now)
    assertEquals(tick.index, 0L)
  }

  test("2.Tick nextTick should increment index and update times") {
    val t1 = Tick.seed(uuid, zoneId, now)
    val wakeup = now.plusSeconds(5)
    val t2 = t1.nextTick(now, wakeup)

    assertEquals(t2.index, t1.index + 1)
    assertEquals(t2.commence, t1.conclude)
    assertEquals(t2.acquires, now)
    assertEquals(t2.conclude, wakeup)
  }

  test("3.Tick window and active durations should be correct") {
    val t = Tick.seed(uuid, zoneId, now).withConclude(now.plusSeconds(10))
    assertEquals(t.active, Duration.between(t.commence, t.acquires))
    assertEquals(t.snooze, Duration.between(t.acquires, t.conclude))
    assertEquals(t.window, Duration.between(t.commence, t.conclude))
  }

  test("4.Tick isWithinOpenClosed and isWithinClosedOpen") {
    val t = Tick.seed(uuid, zoneId, now).withConclude(now.plusSeconds(10))

    assertEquals(t.isWithinOpenClosed(now), false)
    assertEquals(t.isWithinOpenClosed(t.conclude), true)
    assertEquals(t.isWithinClosedOpen(now), true)
    assertEquals(t.isWithinClosedOpen(t.conclude), false)
  }

  test("5.Tick withSnoozeStretch updates conclude") {
    val t = Tick.seed(uuid, zoneId, now)
    val stretched = t.withSnoozeStretch(Duration.ofSeconds(5))
    assertEquals(stretched.conclude, t.conclude.plusSeconds(5))
  }

  test("6.Tick toString uses show interpolator") {
    val t = Tick.seed(uuid, zoneId, now)
    val s = t.toString
    assert(s.contains("id="))
    assert(s.contains("idx=000"))
  }

  test("7.Tick JSON encoding and decoding") {
    val t = Tick.seed(uuid, zoneId, now).withConclude(now.plusSeconds(10))
    val json = t.asJson.noSpaces
    val decoded = decode[Tick](json).toOption.get

    assertEquals(decoded.sequenceId, t.sequenceId)
    assertEquals(decoded.index, t.index)
    assertEquals(decoded.commence, t.commence)
    assertEquals(decoded.acquires, t.acquires)
    assertEquals(decoded.conclude, t.conclude)
  }

  test("8.Tick JSON round-trip preserves local times across DST overlap") {
    val zone = ZoneId.of("Europe/Berlin")
    val t = Tick(
      sequenceId = uuid,
      launchTime = Instant.parse("2026-10-25T00:20:00Z"),
      zoneId = zone,
      index = 7L,
      commence = Instant.parse("2026-10-25T00:30:00Z"),
      acquires = Instant.parse("2026-10-25T01:00:00Z"),
      conclude = Instant.parse("2026-10-25T01:30:00Z")
    )

    val decoded = decode[Tick](t.asJson.noSpaces).toOption.get
    assertEquals(decoded.local(_.launchTime), t.local(_.launchTime))
    assertEquals(decoded.local(_.commence), t.local(_.commence))
    assertEquals(decoded.local(_.acquires), t.local(_.acquires))
    assertEquals(decoded.local(_.conclude), t.local(_.conclude))
  }
}
