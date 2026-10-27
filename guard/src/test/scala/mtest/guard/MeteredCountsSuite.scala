package mtest.guard

import com.github.chenharryhua.nanjin.common.chrono.Tick
import munit.FunSuite

import java.time.{Instant, ZoneId}
import java.util.UUID
import com.github.chenharryhua.nanjin.guard.metrics.{MetricCategory, MetricId}
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.{MeteredCounts, MeteredId}
import io.circe.jawn.decode

object MetricFixtures {

  /** Build a `MeteredId` for a meter named `id`. `age`/`uniqueToken` stamp the per-acquisition identity: two
    * acquisitions of the same name get different stamps, so reusing the default (0,0) models "the same live
    * instrument" while a distinct stamp models a re-registered instrument with the same name.
    */
  private def metric(id: String, age: Long = 0L, uniqueToken: Int = 0): MeteredId = {
    val mid = decode[MetricId](
      s"""{
         |  "scope": {"label": "$id", "domain": "test", "service": "test-service", "task":"task"},
         |  "token": {"metricName": "$id", "age": $age, "uniqueToken": $uniqueToken},
         |  "category": {"Meter": {"kind": {"Default": {}}, "squants": {"unitSymbol": "ea", "dimensionName": "Dimensionless"}}}
         |}""".stripMargin
    ).fold(throw _, identity)
    MeteredId(
      mid.scope,
      mid.token,
      mid.category match {
        case MetricCategory.Meter(_, squants) => squants
        case other => throw new IllegalArgumentException(s"expected Meter category, got: $other")
      }
    )
  }

  // Predefined stable metrics (reused across all tests)
  val a: MeteredId = metric("a")
  val b: MeteredId = metric("b")
  val c: MeteredId = metric("c")
  val d: MeteredId = metric("d")

  /** A meter sharing a name with `a` but minted by a later acquisition (distinct token) — i.e. the same
    * user-facing name after an unregister/re-register.
    */
  val aReacquired: MeteredId = metric("a", age = 1L, uniqueToken = 1)
}
object MeteredTestUtils {

  def tick(ms: Long): Tick =
    Tick.seed(UUID.randomUUID(), ZoneId.of("UTC"), Instant.ofEpochMilli(ms))

  def mc(t: Long, values: (MeteredId, Long)*): MeteredCounts =
    MeteredCounts(
      tick(t),
      values.toMap
    )
}
class MeteredCountsSuite extends FunSuite {

  import MeteredTestUtils.*
  import MetricFixtures.*
  // ----------------------------
  // basic accessors
  // ----------------------------

  test("1.counts returns underlying map") {
    val m = mc(1, a -> 10, b -> 20)

    assert(m.counts == Map(a -> 10, b -> 20))
  }

  // ----------------------------
  // delta semantics
  // ----------------------------

  test("2.delta - basic subtraction") {
    val prev = mc(1, a -> 10, b -> 20)
    val curr = mc(2, a -> 15, b -> 25)

    val result = curr.delta(prev)

    assert(
      result.counts == Map(
        a -> 5,
        b -> 5
      ))
  }

  test("3.delta - new key uses full value") {
    val prev = mc(1, a -> 10)
    val curr = mc(2, a -> 15, b -> 7)

    val result = curr.delta(prev)

    assert(
      result.counts == Map(
        a -> 5,
        b -> 7
      ))
  }

  test("4.delta - missing key dropped") {
    val prev = mc(1, a -> 10, b -> 20)
    val curr = mc(2, a -> 15)

    val result = curr.delta(prev)

    assert(
      result.counts == Map(
        a -> 5
      ))
  }

  test("5.delta - negative values allowed") {
    val prev = mc(1, a -> 10)
    val curr = mc(2, a -> 7)

    val result = curr.delta(prev)

    assert(result.counts(a) == -3)
  }

  // ----------------------------
  // multi-key stability
  // ----------------------------

  test("6.delta - multiple metrics stable") {
    val prev = mc(1, a -> 100, b -> 200, c -> 300)
    val curr = mc(2, a -> 110, b -> 190, c -> 350)

    val result = curr.delta(prev)

    assert(
      result.counts == Map(
        a -> 10,
        b -> -10,
        c -> 50
      ))
  }
  test("7.delta - both empty") {
    val prev = mc(1)
    val curr = mc(2)

    val result = curr.delta(prev)

    assert(result.counts.isEmpty)
  }

  test("8.delta - previous empty") {
    val prev = mc(1)
    val curr = mc(2, a -> 10, b -> 20)

    val result = curr.delta(prev)

    assert(
      result.counts == Map(
        a -> 10,
        b -> 20
      ))
  }

  test("9.delta - current empty drops everything") {
    val prev = mc(1, a -> 10, b -> 20)
    val curr = mc(2)

    val result = curr.delta(prev)

    assert(result.counts.isEmpty)
  }

  // ----------------------------
  // unregister / re-register with the same name (report item V3)
  //
  // A re-registered meter gets a fresh MetricToken, so its MeteredId differs from the retired one even
  // though the user-facing name is identical. delta therefore keys on identity, not name: the retired id
  // vanishes and the fresh id reports its full (small, post-reset) count, never a spurious negative delta.
  // ----------------------------

  test("10.delta - a re-registered meter (same name, new token) is treated as new, not negative") {
    // prev saw the original `a` with a high cumulative count; curr sees the re-acquired `a` (new token)
    // starting from a small count. Because the ids differ, curr.delta(prev) reports the full new count
    // rather than `5 - 1000 = -995`.
    val prev = mc(1, a -> 1000)
    val curr = mc(2, aReacquired -> 5)

    val result = curr.delta(prev)

    assertEquals(result.counts, Map(aReacquired -> 5L))
    // specifically: no negative delta is produced for the re-registered meter
    assert(result.counts.values.forall(_ >= 0L))
  }

  test("11.delta - old and re-registered meter coexisting both report correctly") {
    // if, within one window, both the retiring `a` and its replacement `aReacquired` are present, each is
    // tracked by its own id: `a` deltas against prev, `aReacquired` is new and reports its full count.
    val prev = mc(1, a -> 1000)
    val curr = mc(2, a -> 1003, aReacquired -> 7)

    val result = curr.delta(prev)

    assertEquals(result.counts, Map(a -> 3L, aReacquired -> 7L))
  }

  test("12.MeteredId identity: same name but different token are distinct keys") {
    // guards the premise of the two tests above: name equality does not imply MeteredId equality.
    assert(a != aReacquired)
    assertEquals(Map(a -> 1L, aReacquired -> 2L).size, 2)
  }

}
