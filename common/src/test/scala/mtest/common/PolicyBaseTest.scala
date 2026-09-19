package mtest.common

import cats.effect.IO
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.common.sequence.*
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import munit.CatsEffectSuite

import java.time.{DayOfWeek, Month}
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps
class PolicyBaseTest extends CatsEffectSuite {

  test("1.equality") {
    assert(Policy.crontab(_.every5Minutes).eqv(Policy.crontab(_.every5Minutes)))
    assert(
      Policy.crontab(crontabs.hourly).jitter(1.second)
        .eqv(Policy.crontab(_.hourly).jitter(1.second)))
    assert(!Policy.fixedRate(1.second).eqv(Policy.fixedDelay(1.second)))

    assert(Policy.empty.eqv(Policy.empty))

    assert(Policy.fixedDelay(1.second, 2.second).eqv(Policy.fixedDelay(1.second, 2.second)))

  }

  test("2.fibonacci") {
    assert(fibonacci.take(10).toList == List(1L, 1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L, 55L))
    assert(exponential.take(10).toList == List(1L, 2L, 4L, 8L, 16L, 32L, 64L, 128L, 256L, 512L))
    assert(primes.take(10).toList == List(2L, 3L, 5L, 7L, 11L, 13L, 17L, 19L, 23L, 29L))
  }

  test("3.fixed delay") {
    val policy = Policy.fixedDelay(1.second, 0.second).repeat
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    tickStream.testPolicy[IO]((_: Policy.type) => policy).take(5).compile.toList.map { ticks =>
      val List(a1, a2, a3, a4, a5) = ticks: @unchecked

      assert(a1.index == 1)
      assert(a1.snooze == 1.second.toJava)

      assert(a2.index == 2)
      assert(a2.commence == a1.conclude)
      assert(a2.snooze == 0.second.toJava)

      assert(a3.index == 3)
      assert(a3.commence == a2.conclude)
      assert(a3.snooze == 1.second.toJava)

      assert(a4.index == 4)
      assert(a4.commence == a3.conclude)
      assert(a4.snooze == 0.second.toJava)

      assert(a5.index == 5)
      assert(a5.commence == a4.conclude)
      assert(a5.snooze == 1.second.toJava)
      assert(List(a1, a2, a3, a4, a5).forall(t => t.acquires.plus(t.snooze) == t.conclude))
    }
  }

  test("4.fixed rate") {
    val policy = Policy.fixedRate(1.second).repeat
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    tickStream.testPolicy[IO]((_: Policy.type) => policy).take(5).compile.toList.map { ticks =>
      val List(a1, a2, a3, a4, a5) = ticks: @unchecked

      assert(a1.index == 1)
      assert(a1.conclude == a1.acquires.plus(1.seconds.toJava))

      assert(a2.index == 2)
      assert(a2.commence == a1.conclude)
      assert(a2.conclude == a2.commence.plus(1.seconds.toJava))

      assert(a3.index == 3)
      assert(a3.commence == a2.conclude)
      assert(a3.conclude == a3.commence.plus(1.seconds.toJava))

      assert(a4.index == 4)
      assert(a4.commence == a3.conclude)
      assert(a4.conclude == a4.commence.plus(1.seconds.toJava))

      assert(a5.index == 5)
      assert(a5.commence == a4.conclude)
      assert(a5.conclude == a5.commence.plus(1.seconds.toJava))
      assert(List(a1, a2, a3, a4, a5).forall(t => t.acquires.plus(t.snooze) == t.conclude))
    }
  }

  test("5.fixed delays") {
    val policy = Policy.fixedDelay(1.second, 2.seconds, 3.seconds).repeat
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)

    tickStream.testPolicy[IO]((_: Policy.type) => policy).take(7).compile.toList.map { ticks =>
      val List(a1, a2, a3, a4, a5, a6, a7) = ticks: @unchecked

      assert(a1.index == 1)
      assert(a2.index == 2)
      assert(a3.index == 3)
      assert(a4.index == 4)
      assert(a5.index == 5)
      assert(a6.index == 6)
      assert(a7.index == 7)

      assert(a1.snooze == 1.second.toJava)
      assert(a2.snooze == 2.second.toJava)
      assert(a3.snooze == 3.second.toJava)
      assert(a4.snooze == 1.second.toJava)
      assert(a5.snooze == 2.second.toJava)
      assert(a6.snooze == 3.second.toJava)
      assert(a7.snooze == 1.second.toJava)
      assert(List(a1, a2, a3, a4, a5, a6, a7).forall(t => t.acquires.plus(t.snooze) == t.conclude))
    }
  }

  test("6.cron") {
    val policy = Policy.crontab(_.hourly).repeat
    assert(decode[Policy](policy.asJson.noSpaces).toOption.get == policy)
    tickStream.testPolicy[IO]((_: Policy.type) => policy).take(6).compile.toList.map { ticks =>
      val List(a1, a2, a3, a4, a5, a6) = ticks: @unchecked

      assert(a1.index == 1)
      assert(a2.index == 2)
      assert(a3.index == 3)
      assert(a4.index == 4)
      assert(a5.index == 5)
      assert(a6.index == 6)

      assert(a2.window == 1.hour.toJava)
      assert(a3.window == 1.hour.toJava)
      assert(a4.window == 1.hour.toJava)
      assert(a5.window == 1.hour.toJava)
      assert(a6.window == 1.hour.toJava)
    }
  }

  test("7.weekly") {
    def dayOf(day: crontabs.weekly.type => cron4s.CronExpr): IO[DayOfWeek] =
      tickStream.testPolicy[IO](_.crontab(c => day(c.weekly)))
        .take(1).compile.lastOrError.map(t => DayOfWeek.from(t.zoned(_.conclude)))

    for {
      sunday <- dayOf(_.sunday)
      monday <- dayOf(_.monday)
      tuesday <- dayOf(_.tuesday)
      wednesday <- dayOf(_.wednesday)
      thursday <- dayOf(_.thursday)
      friday <- dayOf(_.friday)
      saturday <- dayOf(_.saturday)
    } yield {
      assertEquals(sunday, DayOfWeek.SUNDAY)
      assertEquals(monday, DayOfWeek.MONDAY)
      assertEquals(tuesday, DayOfWeek.TUESDAY)
      assertEquals(wednesday, DayOfWeek.WEDNESDAY)
      assertEquals(thursday, DayOfWeek.THURSDAY)
      assertEquals(friday, DayOfWeek.FRIDAY)
      assertEquals(saturday, DayOfWeek.SATURDAY)
    }
  }

  test("8.yearly") {
    def monthOf(month: crontabs.yearly.type => cron4s.CronExpr): IO[Month] =
      tickStream.testPolicy[IO](_.crontab(c => month(c.yearly)))
        .take(1).compile.lastOrError.map(t => Month.from(t.zoned(_.conclude)))

    for {
      january <- monthOf(_.january)
      february <- monthOf(_.february)
      march <- monthOf(_.march)
      april <- monthOf(_.april)
      may <- monthOf(_.may)
      june <- monthOf(_.june)
      july <- monthOf(_.july)
      august <- monthOf(_.august)
      september <- monthOf(_.september)
      october <- monthOf(_.october)
      november <- monthOf(_.november)
      december <- monthOf(_.december)
    } yield {
      assertEquals(january, Month.JANUARY)
      assertEquals(february, Month.FEBRUARY)
      assertEquals(march, Month.MARCH)
      assertEquals(april, Month.APRIL)
      assertEquals(may, Month.MAY)
      assertEquals(june, Month.JUNE)
      assertEquals(july, Month.JULY)
      assertEquals(august, Month.AUGUST)
      assertEquals(september, Month.SEPTEMBER)
      assertEquals(october, Month.OCTOBER)
      assertEquals(november, Month.NOVEMBER)
      assertEquals(december, Month.DECEMBER)
    }
  }

  test("9.invalid policy arguments") {
    intercept[IllegalArgumentException] {
      Policy.fixedDelay(0.second, 0.second)
    }

    intercept[IllegalArgumentException] {
      Policy.fixedDelay((-1).second, 1.second)
    }

    intercept[IllegalArgumentException] {
      Policy.fixedRate(0.second)
    }

    intercept[IllegalArgumentException] {
      Policy.fixedRate((-1).second)
    }

    intercept[IllegalArgumentException] {
      Policy.empty.jitter(1.second, 1.second)
    }

    intercept[IllegalArgumentException] {
      Policy.empty.jitter((-1).second, 1.second)
    }
  }

  test("10.fixedDelay with an empty list is Policy.empty") {
    assert(Policy.fixedDelay(List.empty[scala.concurrent.duration.FiniteDuration]).eqv(Policy.empty))
    // a non-empty list still enforces the positivity invariant
    intercept[IllegalArgumentException] {
      Policy.fixedDelay(List(0.second, 0.second))
    }
  }
}
