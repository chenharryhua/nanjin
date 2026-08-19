package mtest.common

import cats.effect.IO
import cats.effect.kernel.Ref
import com.github.chenharryhua.nanjin.common.resilience.Retry
import munit.CatsEffectSuite

import java.time.ZoneId
import scala.collection.mutable
import scala.concurrent.duration.*

class RetrySpec extends CatsEffectSuite {

  test("1.Retry: effect succeeds after failures") {
    val zoneId = ZoneId.systemDefault()
    val maxAttempts = 3
    val state = mutable.ListBuffer.empty[String]

    // fail first 2 times, succeed 3rd
    var counter = 0
    val riskyOp: IO[String] = IO {
      counter += 1
      state += s"attempt $counter"
      if (counter < 3) throw new RuntimeException(s"fail $counter")
      else "success"
    }

    val retryIO = Retry[IO](zoneId, _.withPolicy(_.fixedDelay(100.millis).limited(maxAttempts)))

    retryIO.flatMap { retry =>
      retry(riskyOp).map { result =>
        assertEquals(result, "success")
        assertEquals(state.toList, List("attempt 1", "attempt 2", "attempt 3"))
      }
    }
  }

  test("2.Retry: fails after exhausting policy, only last failure propagated") {
    val zoneId = ZoneId.systemDefault()
    val maxAttempts = 2
    var counter = 0

    val riskyOp: IO[String] = IO {
      counter += 1
      throw new RuntimeException(s"fail $counter")
    }

    val retryIO = Retry[IO](zoneId, _.withPolicy(_.fixedDelay(50.millis).limited(maxAttempts)))

    retryIO.flatMap { retry =>
      retry(riskyOp).attempt.map {
        case Left(ex) =>
          assertEquals(ex.getMessage, "fail 3") // only last failure
          assertEquals(counter, 3)
        case Right(_) => fail("Expected failure, got success")
      }
    }
  }

  test("3.Retry: decision can stop retry early") {
    val zoneId = ZoneId.systemDefault()
    var counter = 0
    val riskyOp: IO[String] = IO {
      counter += 1
      throw new RuntimeException(s"fail $counter")
    }

    val retryIO = Retry[IO](
      zoneId,
      _.withDecision { tv =>
        // Stop retrying after first failure
        IO.pure(tv.giveUp)
      })

    retryIO.flatMap { retry =>
      retry(riskyOp).attempt.map {
        case Left(ex) =>
          assertEquals(ex.getMessage, "fail 1")
          assertEquals(counter, 1)
        case Right(_) => fail("Expected failure")
      }
    }
  }

  test("4.Retry: empty policy should not retry") {
    val zoneId = ZoneId.systemDefault()
    var counter = 0

    val riskyOp: IO[String] = IO {
      counter += 1
      throw new RuntimeException(s"fail $counter")
    }

    val retryIO = Retry[IO](zoneId, identity)

    retryIO.flatMap { retry =>
      retry(riskyOp).attempt.map {
        case Left(ex) =>
          assertEquals(ex.getMessage, "fail 1")
          assertEquals(counter, 1)
        case Right(_) => fail("Expected failure")
      }
    }
  }

  test("5.Retry: decision should not be called when effect succeeds immediately") {
    val zoneId = ZoneId.systemDefault()

    val prom = for {
      decisionCalls <- Ref.of[IO, Int](0)
      retry <- Retry[IO](
        zoneId,
        _.withPolicy(_.fixedDelay(20.millis).limited(3)).withDecision { tv =>
          decisionCalls.update(_ + 1).as(tv.followPolicy)
        })
      result <- retry(IO.pure("ok"))
      calls <- decisionCalls.get
    } yield {
      assertEquals(result, "ok")
      assertEquals(calls, 0)
    }

    prom
  }

  test("6.Retry: decision receives increasing tick indexes") {
    val zoneId = ZoneId.systemDefault()

    val prom = for {
      counter <- Ref.of[IO, Int](0)
      observed <- Ref.of[IO, List[Long]](Nil)
      retry <- Retry[IO](
        zoneId,
        _.withPolicy(_.fixedDelay(10.millis).limited(3)).withDecision { tv =>
          observed.update(_ :+ tv.ordinal).as(tv.followPolicy)
        })
      _ <- retry(
        counter.updateAndGet(_ + 1).flatMap { n =>
          if (n <= 3) IO.raiseError(new RuntimeException(s"boom $n"))
          else IO.pure("ok")
        }
      )
      indexes <- observed.get
    } yield assertEquals(indexes, List(1L, 2L, 3L))

    prom
  }

  test("7.Retry: decision failure should preserve original operation failure") {
    val zoneId = ZoneId.systemDefault()

    val retryIO = Retry[IO](
      zoneId,
      _.withPolicy(_.fixedDelay(10.millis).limited(1)).withDecision { _ =>
        IO.raiseError(new RuntimeException("decision boom"))
      })

    retryIO.flatMap { retry =>
      retry(IO.raiseError[String](new RuntimeException("operation boom"))).attempt.map {
        case Left(ex) =>
          assertEquals(ex.getMessage, "operation boom")
          assert(ex.getSuppressed.exists(_.getMessage == "decision boom"))
        case Right(_) => fail("Expected failure")
      }
    }
  }

  test("8.Retry: decision retryAfter can override policy delay") {
    val zoneId = ZoneId.systemDefault()

    val prom = for {
      invokedAt <- Ref.of[IO, List[Long]](Nil)
      attempts <- Ref.of[IO, Int](0)
      retry <- Retry[IO](
        zoneId,
        _.withPolicy(_.fixedDelay(2.seconds).limited(2)).withDecision { tv =>
          IO.pure(tv.retryAfter(20.millis))
        })
      result <- retry {
        IO(System.nanoTime()).flatMap { ts =>
          invokedAt.update(_ :+ ts) >>
            attempts.updateAndGet(_ + 1).flatMap { n =>
              if (n == 1) IO.raiseError[String](new RuntimeException("boom"))
              else IO.pure("ok")
            }
        }
      }
      history <- invokedAt.get
      List(first, second) = history
      elapsed = (second - first).nanos
    } yield {
      assertEquals(result, "ok")
      assert(elapsed < 500.millis, s"retryAfter should override 2s policy delay, observed $elapsed")
    }

    prom
  }

  test("9.Retry: decision can observe attempt cause retries and snooze") {
    val zoneId = ZoneId.systemDefault()

    val prom = for {
      observedCause <- Ref.of[IO, String]("")
      observedRetries <- Ref.of[IO, Long](-1L)
      observedSnooze <- Ref.of[IO, FiniteDuration](Duration.Zero)
      retry <- Retry[IO](
        zoneId,
        _.withPolicy(_.fixedDelay(25.millis).limited(3)).withDecision { tv =>
          observedCause.set(tv.cause.getMessage) >>
            observedRetries.set(tv.ordinal) >>
            observedSnooze.set(tv.snooze) >>
            IO.pure(tv.giveUp)
        }
      )
      ex <- retry(IO.raiseError[String](new RuntimeException("bad-1"))).attempt.map(_.swap.toOption.get)
      cause <- observedCause.get
      retries <- observedRetries.get
      snooze <- observedSnooze.get
    } yield {
      assertEquals(ex.getMessage, "bad-1")
      assertEquals(cause, "bad-1")
      assertEquals(retries, 1L)
      assertEquals(snooze, 25.millis)
    }

    prom
  }

  test("10.Retry: decision can observe failedAt") {
    val zoneId = ZoneId.systemDefault()

    val prom = for {
      observedFailedAt <- Ref.of[IO, Option[java.time.Instant]](None)
      start <- IO.realTimeInstant
      retry <- Retry[IO](
        zoneId,
        _.withPolicy(_.fixedDelay(20.millis).limited(2)).withDecision { tv =>
          observedFailedAt.set(Some(tv.failedAt.toInstant)) >> IO.pure(tv.giveUp)
        }
      )
      _ <- retry(IO.raiseError[String](new RuntimeException("failed-at"))).attempt
      end <- IO.realTimeInstant
      failedAt <- observedFailedAt.get
    } yield {
      assert(failedAt.nonEmpty)
      val ts = failedAt.get
      assert(!ts.isBefore(start), s"failedAt should be >= start, got $ts < $start")
      assert(!ts.isAfter(end), s"failedAt should be <= end, got $ts > $end")
    }

    prom
  }

  test("11.Retry: previousCause is None on first attempt, Some thereafter") {
    val zoneId = ZoneId.systemDefault()

    val prom = for {
      observed <- Ref.of[IO, List[Option[String]]](Nil)
      attempts <- Ref.of[IO, Int](0)
      retry <- Retry[IO](
        zoneId,
        _.withPolicy(_.fixedDelay(10.millis).limited(3)).withDecision { tv =>
          observed.update(_ :+ tv.previousCause.map(_.getMessage)).as(tv.followPolicy)
        }
      )
      _ <- retry {
        attempts.updateAndGet(_ + 1).flatMap { n =>
          IO.raiseError[String](new RuntimeException(s"err-$n"))
        }
      }.attempt
      history <- observed.get
    } yield {
      assertEquals(history.head, None) // first attempt has no previous
      assertEquals(history(1), Some("err-1")) // second sees first error
      assertEquals(history(2), Some("err-2")) // third sees second error
    }

    prom
  }

  test("12.Retry: elapsed grows across attempts") {
    val zoneId = ZoneId.systemDefault()

    val prom = for {
      observed <- Ref.of[IO, List[FiniteDuration]](Nil)
      attempts <- Ref.of[IO, Int](0)
      retry <- Retry[IO](
        zoneId,
        _.withPolicy(_.fixedDelay(50.millis).limited(2)).withDecision { tv =>
          observed.update(_ :+ tv.elapsed).as(tv.followPolicy)
        })
      _ <- retry {
        attempts.updateAndGet(_ + 1).flatMap { n =>
          IO.raiseError[String](new RuntimeException(s"err-$n"))
        }
      }.attempt
      history <- observed.get
    } yield {
      // Each elapsed should be non-negative and non-decreasing
      assert(history.forall(_ >= Duration.Zero))
      assert(history.size == 2)
      assert(history(1) >= history(0))
    }

    prom
  }

  test("13.Retry: reusable across multiple calls with independent elapsed") {
    val zoneId = ZoneId.systemDefault()

    val prom = for {
      observed <- Ref.of[IO, List[FiniteDuration]](Nil)
      retry <- Retry[IO](
        zoneId,
        _.withPolicy(_.fixedDelay(10.millis).limited(1)).withDecision { tv =>
          observed.update(_ :+ tv.elapsed).as(tv.followPolicy)
        })
      _ <- retry(IO.raiseError[String](new RuntimeException("a"))).attempt
      _ <- retry(IO.raiseError[String](new RuntimeException("b"))).attempt
      elapsed <- observed.get
    } yield {
      assertEquals(elapsed.size, 2)
      assert(elapsed.forall(_.toMillis < 500))
    }

    prom
  }

  test("14.Retry: Decision.accepted is true for followPolicy") {
    val zoneId = ZoneId.systemDefault()

    val prom = for {
      observedAccepted <- Ref.of[IO, List[Boolean]](Nil)
      retry <- Retry[IO](
        zoneId,
        _.withPolicy(_.fixedDelay(10.millis).limited(2)).withDecision { tv =>
          val decision = tv.followPolicy
          observedAccepted.update(_ :+ decision.accepted).as(decision)
        }
      )
      _ <- retry {
        IO.raiseError[String](new RuntimeException("boom"))
      }.attempt
      history <- observedAccepted.get
    } yield {
      assert(history.nonEmpty)
      assert(history.forall(_ == true))
    }

    prom
  }

  test("15.Retry: Decision.accepted is false for giveUp") {
    val zoneId = ZoneId.systemDefault()

    val prom = for {
      observedAccepted <- Ref.of[IO, Option[Boolean]](None)
      retry <- Retry[IO](
        zoneId,
        _.withPolicy(_.fixedDelay(10.millis).limited(2)).withDecision { tv =>
          val decision = tv.giveUp
          observedAccepted.set(Some(decision.accepted)).as(decision)
        }
      )
      _ <- retry(IO.raiseError[String](new RuntimeException("boom"))).attempt
      accepted <- observedAccepted.get
    } yield assertEquals(accepted, Some(false))

    prom
  }

  test("16.Retry: Decision encoder produces correct JSON for followPolicy") {
    import io.circe.Encoder

    val zoneId = ZoneId.systemDefault()

    val prom = for {
      observedJson <- Ref.of[IO, Option[io.circe.Json]](None)
      retry <- Retry[IO](
        zoneId,
        _.withPolicy(_.fixedDelay(25.millis).limited(2)).withDecision { tv =>
          val decision = tv.followPolicy
          val json = Encoder[Retry.Decision].apply(decision)
          observedJson.set(Some(json)).as(decision)
        }
      )
      _ <- retry(IO.raiseError[String](new RuntimeException("boom"))).attempt
      json <- observedJson.get
    } yield {
      assert(json.nonEmpty)
      val j = json.get
      assertEquals(j.hcursor.get[Boolean]("retry").toOption, Some(true))
      assert(j.hcursor.get[String]("failed_at").isRight)
      assert(j.hcursor.get[String]("wakeup_at").isRight)
      assert(j.hcursor.get[String]("snooze").isRight)
      assert(j.hcursor.get[Long]("ordinal").isRight)
      assert(j.hcursor.get[String]("zone_id").isRight)
    }

    prom
  }

  test("17.Retry: Decision encoder produces correct JSON for giveUp") {
    import io.circe.Encoder

    val zoneId = ZoneId.systemDefault()

    val prom = for {
      observedJson <- Ref.of[IO, Option[io.circe.Json]](None)
      retry <- Retry[IO](
        zoneId,
        _.withPolicy(_.fixedDelay(25.millis).limited(2)).withDecision { tv =>
          val decision = tv.giveUp
          val json = Encoder[Retry.Decision].apply(decision)
          observedJson.set(Some(json)).as(decision)
        }
      )
      _ <- retry(IO.raiseError[String](new RuntimeException("boom"))).attempt
      json <- observedJson.get
    } yield {
      assert(json.nonEmpty)
      val j = json.get
      assertEquals(j.hcursor.get[Boolean]("retry").toOption, Some(false))
      assert(j.hcursor.get[String]("failed_at").isRight)
      assert(j.hcursor.get[Long]("ordinal").isRight)
      assert(j.hcursor.get[String]("zone_id").isRight)
      // giveUp should NOT have wakeup_at or snooze
      assert(j.hcursor.get[String]("wakeup_at").isLeft)
      assert(j.hcursor.get[String]("snooze").isLeft)
    }

    prom
  }
}
