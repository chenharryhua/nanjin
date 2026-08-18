# nj-common

Foundational library providing scheduling, resilience, logging, and utility types shared across all nanjin modules.

## Modules

### chrono — Scheduling and Time

A composable policy-based scheduling system built on [Droste](https://github.com/higherkindness/droste) recursion schemes and [fs2](https://fs2.io) streams.

**Policy** — An algebraic data type for defining scheduling strategies:

```scala
import com.github.chenharryhua.nanjin.common.chrono.*
import scala.concurrent.duration.*

// Fixed delay between ticks
val p1 = Policy.fixedDelay(5.seconds)

// Fixed rate (wall-clock aligned)
val p2 = Policy.fixedRate(1.minute)

// Cron expression
val p3 = Policy.crontab(_.hourly)

// Combinators
val p4 = Policy.fixedDelay(1.second, 2.seconds, 4.seconds) // escalating delays
  .limited(10)           // at most 10 ticks
  .jitter(0.millis, 500.millis)  // random jitter per tick
  .followedBy(Policy.fixedDelay(30.seconds)) // switch policy after exhaustion
  .repeat              // restart from the beginning when exhausted

// Exclusions and offsets
val p5 = Policy.crontab(_.hourly).except(localTimes.noon).offset(100.millis)
```

**Tick** — A structured time-frame with identity, temporal boundaries, and evolution:

```
commence  -->  acquires  -->  conclude
    |              |--- snooze ---|
    |--- active ---|
    |----------- window ---------|
```

**tickStream** — fs2 streams driven by policies:

```scala
import com.github.chenharryhua.nanjin.common.chrono.tickStream

// Sleep first, then emit
tickStream.tickScheduled[IO](zoneId, _.fixedDelay(5.seconds))

// Emit first, then sleep to next
tickStream.tickFuture[IO](zoneId, _.crontab(_.hourly))
```

**Named constants** — `localTimes` (midnight through elevenPM), `zones` (UTC, Sydney, Beijing, etc.), `crontabs` (secondly, minutely, hourly, daily, etc.)

### resilience — Fault Tolerance

**CircuitBreaker** — State machine (Closed/HalfOpen/Open) protecting effectful computations:

```scala
import com.github.chenharryhua.nanjin.common.resilience.CircuitBreaker

CircuitBreaker[IO](zoneId, maxFailures = 5, _.fixedDelay(10.seconds)).use { cb =>
  cb.protect(riskyCall)
}
```

**Retry** — Policy-driven retry with a single decision function that can follow the policy, override the delay, or give up — based on the exception type, ordinal, elapsed time, or previous failures:

```scala
import com.github.chenharryhua.nanjin.common.resilience.Retry

Retry[IO](zoneId, _.withPolicy(_.fixedDelay(1.second).limited(10)).withDecision { attempt =>
  IO {
    attempt.cause match {
      case _: RateLimitException  => attempt.retryAfter(30.seconds) // server said back off
      case _: TransientException if attempt.ordinal < 5 => attempt.followPolicy
      case _ if attempt.elapsed.toMinutes > 2 => attempt.giveUp // enough time spent
      case _ => attempt.giveUp
    }
  }
}).flatMap(retry => retry(action))
```

The `Attempt` provides: `cause`, `previousCause`, `ordinal`, `elapsed: FiniteDuration` (real wall-clock time since first failure, including both sleep and execution time), `snooze: FiniteDuration`, and `failedAt`.

**SingleFlight** — Deduplicates concurrent effectful calls (leader/follower pattern):

```scala
import com.github.chenharryhua.nanjin.common.resilience.SingleFlight

SingleFlight[IO, Result].flatMap { sf =>
  // Only one computation runs; others wait for the same result
  sf(expensiveComputation)
}
```

### logging — Structured Logging

**Log[F]** — Abstract effectful logger with level-based filtering:

```scala
import com.github.chenharryhua.nanjin.common.logging.{Log, LogLevel}

// Levels: Error > Warn > Good > Info > Debug
log.error("critical failure", exception)
log.warn("degraded state")
log.good("operation succeeded")
log.info("status update")
log.debug(computeDebugInfo)  // effectful debug
```

**LogLevel** — Enum with `Error`, `Warn`, `Good`, `Info`, `Debug`. Includes circe codecs, Show, and Order instances.

### xml — XML/JSON Bidirectional Codec

Converts between `scala.xml.Node` and `io.circe.Json`, handling attributes, mixed content, and repeated children.

### Core Utilities

**DurationFormatter** — Human-readable duration formatting with configurable granularity:

```scala
import com.github.chenharryhua.nanjin.common.DurationFormatter

DurationFormatter.defaultFormatter.format(Duration.ofSeconds(3723))
// "1 hour 2 minutes"

DurationFormatter.create(4).format(Duration.ofSeconds(3723))
// "1 hour 2 minutes 3 seconds"
```

**ChunkSize** — Validated positive integer opaque type with circe/Show/Order instances.

**sequence** — Lazy mathematical sequences:

```scala
import com.github.chenharryhua.nanjin.common.sequence.*

fibonacci.take(5).toList   // List(1, 1, 2, 3, 5)
exponential.take(5).toList // List(1, 2, 4, 8, 16)
primes.take(5).toList      // List(2, 3, 5, 7, 11)
```

**fixpoint** — Relates Monocle's `Plated` typeclass to Droste's `Fix`, `Attr`, and `Coattr` for recursive data structure traversal.

**TypeName[A]** — Compile-time macro providing a type's short name.

**OpaqueLift** — Lifts typeclass instances from a representation type to an opaque type.

**UpdateConfig / EnableConfig / HasProperties** — Mixin traits for configuration builder patterns.

## Dependencies

- Cats / Cats Effect / fs2
- Droste (recursion schemes)
- Monocle (optics)
- Circe (JSON)
- Cron4s (cron expressions)
- Squants (units of measure)
- scala-xml

## Artifact

```
"com.github.chenharryhua" %% "nj-common" % version
```
