package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.service.MeteredCounts
import org.scalatest.funsuite.AnyFunSuite
import squants.Each

import java.time.{Instant, ZoneId}
import java.util.UUID

object MetricFixtures {

  def metric(id: String): MetricID =
    MetricID(
      MetricLabel(id, Domain("test")),
      MetricName[IO](id).unsafeRunSync(),
      Category.Meter(MeterKind.Meter, Squants(Each))
    )

  // Predefined stable metrics (reused across all tests)
  val a: MetricID = metric("a")
  val b: MetricID = metric("b")
  val c: MetricID = metric("c")
  val d: MetricID = metric("d")
}
object MeteredTestUtils {

  def tick(ms: Long): Tick =
    Tick.zeroth(UUID.randomUUID(), ZoneId.of("UTC"), Instant.ofEpochMilli(ms))

  def mc(t: Long, values: (MetricID, Long)*): MeteredCounts =
    MeteredCounts(
      tick(t),
      values.toMap
    )
}
class MeteredCountsSuite extends AnyFunSuite {

  import MeteredTestUtils.*
  import MetricFixtures.*
  // ----------------------------
  // basic accessors
  // ----------------------------

  test("counts returns underlying map") {
    val m = mc(1, a -> 10, b -> 20)

    assert(m.counts == Map(a -> 10, b -> 20))
  }

  // ----------------------------
  // delta semantics
  // ----------------------------

  test("delta - basic subtraction") {
    val prev = mc(1, a -> 10, b -> 20)
    val curr = mc(2, a -> 15, b -> 25)

    val result = curr.delta(prev)

    assert(
      result.counts == Map(
        a -> 5,
        b -> 5
      ))
  }

  test("delta - new key uses full value") {
    val prev = mc(1, a -> 10)
    val curr = mc(2, a -> 15, b -> 7)

    val result = curr.delta(prev)

    assert(
      result.counts == Map(
        a -> 5,
        b -> 7
      ))
  }

  test("delta - missing key dropped") {
    val prev = mc(1, a -> 10, b -> 20)
    val curr = mc(2, a -> 15)

    val result = curr.delta(prev)

    assert(
      result.counts == Map(
        a -> 5
      ))
  }

  test("delta - negative values allowed") {
    val prev = mc(1, a -> 10)
    val curr = mc(2, a -> 7)

    val result = curr.delta(prev)

    assert(result.counts(a) == -3)
  }

  // ----------------------------
  // multi-key stability
  // ----------------------------

  test("delta - multiple metrics stable") {
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
  test("delta - both empty") {
    val prev = mc(1)
    val curr = mc(2)

    val result = curr.delta(prev)

    assert(result.counts.isEmpty)
  }

  test("delta - previous empty") {
    val prev = mc(1)
    val curr = mc(2, a -> 10, b -> 20)

    val result = curr.delta(prev)

    assert(
      result.counts == Map(
        a -> 10,
        b -> 20
      ))
  }

  test("delta - current empty drops everything") {
    val prev = mc(1, a -> 10, b -> 20)
    val curr = mc(2)

    val result = curr.delta(prev)

    assert(result.counts.isEmpty)
  }

}
