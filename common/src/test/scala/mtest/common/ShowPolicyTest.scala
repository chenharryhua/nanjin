package mtest.common

import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.common.chrono.Policy
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalTime
import scala.concurrent.duration.DurationInt

/** Rendering tests for `ShowPolicy`, exercised through `Show[Policy]`/`toString` (the object itself is
  * package-private). Covers every `PolicyF` node: the four leaf schedules and all seven operator nodes, plus
  * nesting/precedence and the leaf value formats (ISO-8601 `Duration`, `LocalTime`, raw cron string).
  */
class ShowPolicyTest extends AnyFunSuite {

  // ---- leaf schedules ------------------------------------------------------------------------------

  test("1.empty renders as the empty label") {
    assert(Policy.empty.show == "empty")
  }

  test("2.fixedDelay with a single delay renders the ISO-8601 duration") {
    assert(Policy.fixedDelay(1.second).show == "fixedDelay(PT1S)")
  }

  test("3.fixedDelay with multiple delays joins them with a comma, in order") {
    assert(Policy.fixedDelay(1.second, 2.seconds, 500.millis).show == "fixedDelay(PT1S,PT2S,PT0.5S)")
  }

  test("4.fixedDelay of an empty list collapses to empty") {
    // an empty delay list is another way to say `empty` (see Policy.fixedDelay docs)
    assert(Policy.fixedDelay(List.empty).show == "empty")
  }

  test("5.fixedRate renders the ISO-8601 duration") {
    assert(Policy.fixedRate(5.seconds).show == "fixedRate(PT5S)")
  }

  test("6.crontab renders the raw cron expression") {
    assert(Policy.crontab(_.every5Minutes).show == "crontab(0 */5 * ? * *)")
  }

  // ---- operator nodes ------------------------------------------------------------------------------

  test("7.limited appends .limited(n)") {
    assert(Policy.fixedDelay(1.second).limited(3).show == "fixedDelay(PT1S).limited(3)")
  }

  test("8.repeat appends .repeat") {
    assert(Policy.fixedDelay(1.second).repeat.show == "fixedDelay(PT1S).repeat")
  }

  test("9.followedBy nests the follower policy") {
    assert(
      Policy.fixedDelay(1.second).followedBy(Policy.fixedRate(2.seconds)).show ==
        "fixedDelay(PT1S).followedBy(fixedRate(PT2S))")
  }

  test("10.meet nests the second policy") {
    assert(
      Policy.fixedDelay(1.second).meet(Policy.fixedRate(2.seconds)).show ==
        "fixedDelay(PT1S).meet(fixedRate(PT2S))")
  }

  test("11.except renders the LocalTime") {
    assert(
      Policy.crontab(_.every5Minutes).except(LocalTime.of(3, 0)).show ==
        "crontab(0 */5 * ? * *).except(03:00:00)")
  }

  test("12.offset appends the ISO-8601 duration") {
    assert(Policy.fixedDelay(1.second).offset(2.seconds).show == "fixedDelay(PT1S).offset(PT2S)")
  }

  test("13.jitter renders min and max, comma-separated") {
    assert(
      Policy.fixedDelay(1.second).jitter(1.second, 3.seconds).show == "fixedDelay(PT1S).jitter(PT1S,PT3S)")
  }

  test("14.jitter with only a max defaults min to zero") {
    assert(Policy.fixedDelay(1.second).jitter(30.seconds).show == "fixedDelay(PT1S).jitter(PT0S,PT30S)")
  }

  // ---- nesting / precedence ------------------------------------------------------------------------

  test("15.chained operators render left-to-right, each wrapping the accumulated policy") {
    assert(
      Policy.crontab(_.every5Minutes).repeat.jitter(30.seconds).limited(10).show ==
        "crontab(0 */5 * ? * *).repeat.jitter(PT0S,PT30S).limited(10)")
  }

  test("16.followedBy composes rendered subpolicies on both sides") {
    val leader = Policy.fixedDelay(1.second).limited(2)
    val follower = Policy.fixedRate(5.seconds).repeat
    assert(
      leader.followedBy(follower).show ==
        "fixedDelay(PT1S).limited(2).followedBy(fixedRate(PT5S).repeat)")
  }

  test("17.Show[Policy] agrees with toString") {
    val policy = Policy.crontab(_.every5Minutes).repeat.jitter(30.seconds)
    assert(policy.show == policy.toString)
  }
}
