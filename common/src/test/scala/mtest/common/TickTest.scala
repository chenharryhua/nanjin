package mtest.common

import com.github.chenharryhua.nanjin.common.chrono.Tick
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.{Duration, Instant, ZoneId}
import java.util.UUID

class TickTest extends AnyFunSuite with Matchers {

  val zoneId: ZoneId = ZoneId.of("UTC")
  val now: Instant = Instant.parse("2026-02-14T00:00:00Z")
  val uuid: UUID = UUID.randomUUID()

  test("Tick zeroth should initialize correctly") {
    val tick = Tick.zeroth(uuid, zoneId, now)
    tick.sequenceId shouldEqual uuid
    tick.launchTime shouldEqual now
    tick.commence shouldEqual now
    tick.acquires shouldEqual now
    tick.conclude shouldEqual now
    tick.index shouldEqual 0L
  }

  test("Tick nextTick should increment index and update times") {
    val t1 = Tick.zeroth(uuid, zoneId, now)
    val wakeup = now.plusSeconds(5)
    val t2 = t1.nextTick(now, wakeup)

    t2.index shouldEqual t1.index + 1
    t2.commence shouldEqual t1.conclude
    t2.acquires shouldEqual now
    t2.conclude shouldEqual wakeup
  }

  test("Tick window and active durations should be correct") {
    val t = Tick.zeroth(uuid, zoneId, now).withConclude(now.plusSeconds(10))
    t.active shouldEqual Duration.between(t.commence, t.acquires)
    t.snooze shouldEqual Duration.between(t.acquires, t.conclude)
    t.window shouldEqual Duration.between(t.commence, t.conclude)
  }

  test("Tick isWithinOpenClosed and isWithinClosedOpen") {
    val t = Tick.zeroth(uuid, zoneId, now).withConclude(now.plusSeconds(10))

    t.isWithinOpenClosed(now) shouldEqual false
    t.isWithinOpenClosed(t.conclude) shouldEqual true
    t.isWithinClosedOpen(now) shouldEqual true
    t.isWithinClosedOpen(t.conclude) shouldEqual false
  }

  test("Tick withSnoozeStretch updates conclude") {
    val t = Tick.zeroth(uuid, zoneId, now)
    val stretched = t.withSnoozeStretch(Duration.ofSeconds(5))
    stretched.conclude shouldEqual t.conclude.plusSeconds(5)
  }

  test("Tick toString uses show interpolator") {
    val t = Tick.zeroth(uuid, zoneId, now)
    val s = t.toString
    s.contains("id=") shouldEqual true
    s.contains("idx=000") shouldEqual true
  }

  test("Tick JSON encoding and decoding") {
    val t = Tick.zeroth(uuid, zoneId, now).withConclude(now.plusSeconds(10))
    val json = t.asJson.noSpaces
    val decoded = decode[Tick](json).toOption.get

    decoded.sequenceId shouldEqual t.sequenceId
    decoded.index shouldEqual t.index
    decoded.commence shouldEqual t.commence
    decoded.acquires shouldEqual t.acquires
    decoded.conclude shouldEqual t.conclude
  }
}
