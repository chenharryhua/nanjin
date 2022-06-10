package mtest.guard

import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.config.{ActionRetryParams, NJRetryPolicy}
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps

class RetryPolicyTest extends AnyFunSuite {
  test("1.max=None,cap=None,policy=GiveUp") {
    val p   = NJRetryPolicy.AlwaysGiveUp
    val res = "alwaysGiveUp"
    assert(ActionRetryParams(None, None, p).policy[IO].show === res)
  }

  test("2.max=Some(1),cap=None,policy=GiveUp") {
    val p   = NJRetryPolicy.AlwaysGiveUp
    val res = "alwaysGiveUp"
    assert(ActionRetryParams(Some(1), None, p).policy[IO].show === res)
  }

  test("3.max=Some(1),cap=None,policy=Constant") {
    val p   = NJRetryPolicy.ConstantDelay(1.second.toJava)
    val res = "constantDelay(1 second).join(limitRetries(maxRetries=1))"
    assert(ActionRetryParams(Some(1), None, p).policy[IO].show === res)
  }

  test("4.max=Some(0),cap=None,policy=Constant") {
    val p   = NJRetryPolicy.ConstantDelay(1.second.toJava)
    val res = "alwaysGiveUp"
    assert(ActionRetryParams(Some(0), None, p).policy[IO].show === res)
  }

  test("5.max=Some(1),cap=Some(1 sec),policy=Constant") {
    val p   = NJRetryPolicy.ConstantDelay(5.second.toJava)
    val res = "constantDelay(5 seconds).meet(constantDelay(1 second)).join(limitRetries(maxRetries=1))"
    assert(ActionRetryParams(Some(1), Some(1.second.toJava), p).policy[IO].show === res)
  }
  test("6.max=Some(1),cap=Some(1 sec),policy=Fib") {
    val p = NJRetryPolicy.FibonacciBackoff(5.second.toJava)
    val res =
      "fibonacciBackoff(baseDelay=5 seconds).meet(constantDelay(1 second)).join(limitRetries(maxRetries=1))"
    assert(ActionRetryParams(Some(1), Some(1.second.toJava), p).policy[IO].show === res)
  }
  test("7.max=Some(1),cap=Some(1 sec),policy=Jitter") {
    val p = NJRetryPolicy.JitterBackoff(2.second.toJava, 5.second.toJava)
    val res =
      "jitterBackoff(minDelay=2 seconds, maxDelay=5 seconds).meet(constantDelay(1 second)).join(limitRetries(maxRetries=1))"
    assert(ActionRetryParams(Some(1), Some(1.second.toJava), p).policy[IO].show === res)
  }
  test("8.max=Some(1),cap=Some(1 sec),policy=Exp") {
    val p = NJRetryPolicy.ExponentialBackoff(2.second.toJava)
    val res =
      "exponentialBackOff(baseDelay=2 seconds).meet(constantDelay(1 second)).join(limitRetries(maxRetries=1))"
    assert(ActionRetryParams(Some(1), Some(1.second.toJava), p).policy[IO].show === res)
  }
  test("9.max=Some(1),cap=Some(1 sec),policy=FullJitter") {
    val p   = NJRetryPolicy.FullJitter(2.second.toJava)
    val res = "fullJitter(baseDelay=2 seconds).meet(constantDelay(1 second)).join(limitRetries(maxRetries=1))"
    assert(ActionRetryParams(Some(1), Some(1.second.toJava), p).policy[IO].show === res)
  }
}
