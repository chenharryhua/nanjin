package mtest.guard

import com.github.chenharryhua.nanjin.common.chrono.Tick
import io.circe.Decoder
import io.circe.jawn.parse
import org.scalatest.funsuite.AnyFunSuite

import java.time.{Instant, ZoneId}
import java.util.UUID
import com.github.chenharryhua.nanjin.guard.metrics.MetricID
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.MeteredCounts

object MetricFixtures {

  def metric(id: String): MetricID =
    parse(
      s"""{
         |  "metricLabel": {"label": "$id", "domain": "test"},
         |  "metricName": {"name": "$id", "age": 0, "uniqueToken": 0},
         |  "category": {"Meter": {
         |    "kind": {"Default": {}},
         |    "squants": {"unitSymbol": "1", "dimensionName": "Dimensionless"}
         |  }}
         |}""".stripMargin
    ).flatMap(_.as[MetricID](using Decoder[MetricID])).fold(throw _, identity)

  // Predefined stable metrics (reused across all tests)
  val a: MetricID = metric("a")
  val b: MetricID = metric("b")
  val c: MetricID = metric("c")
  val d: MetricID = metric("d")
}
object MeteredTestUtils {

  def tick(ms: Long): Tick =
    Tick.seed(UUID.randomUUID(), ZoneId.of("UTC"), Instant.ofEpochMilli(ms))

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

}
