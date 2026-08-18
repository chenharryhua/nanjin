# nj-common

Foundational library providing scheduling, resilience, logging, and utility types shared across all nanjin modules.

## chrono — Scheduling and Time

A composable policy-based scheduling system. `Policy` defines *when* things happen. Consumers (`Retry`, `CircuitBreaker`, `tickStream`, service watchdog, HTTP retry) each define *what* and *why*.

### Policy

An algebraic data type for defining scheduling strategies. Policies are values — immutable, serializable to JSON, composable via combinators.

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

Drawbacks:
- Internally uses Droste's `Fix` recursion scheme — not exposed to users but adds a transitive dependency.
- `fixedRate` catch-up behavior (skipping missed ticks) can surprise users who expect strict 1:1 tick emission.

### Tick

A structured time-frame with identity, temporal boundaries, and evolution. Carries `sequenceId`, `index`, zone, and three instants:

```
commence  -->  acquires  -->  conclude
    |              |--- snooze ---|
    |--- active ---|
    |----------- window ---------|
```

Each `Tick` evolves deterministically via `nextTick`. Ticks are JSON-serializable and zone-aware.

Drawbacks:
- All instants are stored as `java.time.Instant` internally. Conversion to local/zoned representations is derived, which adds a small overhead per access.
- Invariants (ordering, monotonicity) are not runtime-enforced — they rely on correct construction.

### tickStream

fs2 streams driven by policies:

```scala
import com.github.chenharryhua.nanjin.common.chrono.tickStream

// Sleep first, then emit (strict schedule)
tickStream.tickScheduled[IO](zoneId, _.fixedDelay(5.seconds))

// Emit first, then sleep to next (at-least schedule)
tickStream.tickFuture[IO](zoneId, _.crontab(_.hourly))

// Test helper — no real sleeping, jittered clock
tickStream.testPolicy[IO](_.fixedDelay(1.second).limited(5))
```

Drawbacks:
- `tickScheduled` blocks the stream during sleep — downstream can't cancel mid-sleep without fiber cancellation.
- `tickFuture` may emit late ticks if downstream processing overruns the cadence; it doesn't skip.

### Named constants

- `localTimes` — midnight through elevenPM (24 `LocalTime` values, hourly)
- `zones` — UTC, Sydney, Beijing, Singapore, Mumbai, New York, London, Berlin, Cairo, Salta, Darwin
- `crontabs` — secondly, minutely, every3Seconds, every5Minutes, hourly, daily, etc.

---

## resilience — Fault Tolerance

### CircuitBreaker

State machine (Closed → Open → HalfOpen → Closed) protecting effectful computations from repeated failures. Uses `tickStream` internally as the open-to-halfopen timer.

```scala
import com.github.chenharryhua.nanjin.common.resilience.CircuitBreaker

CircuitBreaker[IO](zoneId, maxFailures = 5, _.fixedDelay(10.seconds)).use { cb =>
  cb.protect(riskyCall)     // throws RejectedException when open
  cb.attempt(riskyCall)     // returns Either when open
  cb.getState               // inspect current state
}
```

Drawbacks:
- `maxFailures` counts consecutive failures in Closed state only. A single success resets the counter — there's no sliding window or rate-based opening.
- The breaker is scoped to a `Resource` — it can't outlive the resource's lifetime. For long-lived breakers, the enclosing resource must also be long-lived.
- No half-open concurrency control — multiple fibers can enter half-open simultaneously.

### Retry

Policy-driven retry with a single decision function. Three transitions: `followPolicy`, `retryAfter(delay)`, `giveUp`.

```scala
import com.github.chenharryhua.nanjin.common.resilience.Retry

Retry[IO](zoneId, _.withPolicy(_.fixedDelay(1.second).limited(10)).withDecision { attempt =>
  IO {
    attempt.cause match {
      case _: RateLimitException  => attempt.retryAfter(30.seconds) // server said back off
      case _: TransientException if attempt.ordinal < 5 => attempt.followPolicy
      case _ if attempt.elapsed > 2.minutes => attempt.giveUp // enough time spent
      case _ => attempt.giveUp
    }
  }
}).flatMap(retry => retry(action))
```

The `Attempt` provides:
- `cause: Throwable` — the current exception
- `previousCause: Option[Throwable]` — exception from the prior attempt (`None` on first failure)
- `ordinal: Long` — failure count (1-based)
- `elapsed: FiniteDuration` — real wall-clock time since first failure (includes sleep + execution, not just accumulated policy delays)
- `snooze: FiniteDuration` — the delay the policy proposes
- `failedAt: ZonedDateTime` — timestamp of the failure

Drawbacks:
- Only retries on `Throwable`. For retrying on "bad" successful values (e.g., HTTP 503 response), wrap in an exception or use a domain-specific retry (see `httpRetry`).
- The `Retry[F]` instance is reusable, but `elapsed` tracks time within a single `retry(fa)` call — not across multiple calls to the same instance.
- `retryAfter` overrides the current sleep but doesn't change the policy progression. The next attempt still uses the policy's next scheduled delay.

### SingleFlight

Deduplicates concurrent effectful calls. At most one computation runs at a time; concurrent callers wait for the leader's result.

```scala
import com.github.chenharryhua.nanjin.common.resilience.SingleFlight

SingleFlight[IO, Result].flatMap { sf =>
  sf(expensiveComputation)      // leader runs, followers wait
  sf.tryApply(computation)      // returns None if already in-flight
  sf.isBusy                     // check if a computation is running
}
```

Drawbacks:
- All followers get the same result — including the same error. If the leader fails, all followers fail with the same exception.
- If the leader fiber is canceled, followers receive `LeaderCancelledException` — they don't automatically retry.
- No TTL or cache — each new call after the leader completes starts a fresh computation.

---

## logging — Structured Logging

### Log[F]

Abstract effectful logger with level-based filtering and JSON-encoded messages. Defines an SPI (create/publish/enabled) and a public API (error/warn/good/info/debug).

```scala
import com.github.chenharryhua.nanjin.common.logging.{Log, LogLevel}

// Levels: Error > Warn > Good > Info > Debug
log.error("critical failure", exception)
log.warn("degraded state")
log.good("operation succeeded")
log.info("status update")
log.debug(computeDebugInfo)  // effectful debug — swallows errors gracefully
```

Drawbacks:
- Messages must have a `circe.Encoder` instance. Plain strings work (String has an Encoder), but custom types need deriving.
- `good` is a non-standard log level — sits between Warn and Info. External log aggregators won't recognize it natively.
- The `debug(F[S])` overload catches and swallows computation failures, logging them as debug-level errors. This can hide bugs if misused.

### LogLevel

Enum with `Error`, `Warn`, `Good`, `Info`, `Debug`. Provides circe codecs, Show, and Order instances. `Good` is specific to nanjin — represents "success worth noting" between Warn and Info severity.

---

## xml — XML/JSON Bidirectional Codec

Converts between `scala.xml.Node` and `io.circe.Json`, handling attributes, mixed content, and repeated children.

Drawbacks:
- Attribute handling uses `@attr` convention in JSON — non-standard and requires consumers to know the convention.
- Mixed content (text + elements) uses `#text` and `#children` keys which add structural noise.
- No namespace preservation — namespaces are stripped during conversion.

---

## Core Utilities

### DurationFormatter

Human-readable duration formatting with configurable granularity.

```scala
import com.github.chenharryhua.nanjin.common.DurationFormatter

DurationFormatter.defaultFormatter.format(Duration.ofSeconds(3723))
// "1 hour 2 minutes"

DurationFormatter.create(4).format(Duration.ofSeconds(3723))
// "1 hour 2 minutes 3 seconds"

// Also accepts: Instant pairs, ZonedDateTime pairs, squants Time
DurationFormatter.defaultFormatter.format(startInstant, endInstant)
```

Drawbacks:
- English-only output. No i18n support.
- Uses singular/plural rules (`1 day` vs `2 days`) — not suitable for machine parsing.

### ChunkSize

Validated positive integer opaque type with circe/Show/Order instances. Throws `IllegalArgumentException` on non-positive values.

```scala
import com.github.chenharryhua.nanjin.common.ChunkSize

val cs = ChunkSize(100)       // validated at construction
val cs2: ChunkSize = 100      // implicit conversion from Int (also validated)
```

Drawbacks:
- Validation is eager (throws) rather than returning `Either` or `Validated`. The circe `Decoder` does use `Either` for decoding failures.

### sequence

Lazy mathematical sequences:

```scala
import com.github.chenharryhua.nanjin.common.sequence.*

fibonacci.take(5).toList   // List(1, 1, 2, 3, 5)
exponential.take(5).toList // List(1, 2, 4, 8, 16)
primes.take(5).toList      // List(2, 3, 5, 7, 11)
```

Drawbacks:
- `fibonacci` overflows `Long` at the 93rd element. No `BigInt` variant provided.
- `primes` uses trial division — adequate for small values but O(n√n) per element.

### fixpoint

Relates Monocle's `Plated` typeclass to Droste's `Fix`, `Attr`, and `Coattr` for recursive data structure traversal.

Drawbacks:
- Requires both Monocle and Droste as dependencies. Only useful if you're working with recursion-scheme-based data structures.

### TypeName[A]

Compile-time macro providing a type's short name (the symbol name, not the fully-qualified name).

```scala
import com.github.chenharryhua.nanjin.common.TypeName

TypeName[String].value  // "String"
TypeName[List[Int]].value  // "List"
```

Drawbacks:
- Returns the simple symbol name only — `List[Int]` gives `"List"`, not `"List[Int]"`. Type arguments are lost.

### OpaqueLift

Lifts typeclass instances from a representation type to an opaque type via `asInstanceOf`. Used internally to derive Show/Encoder/Decoder/Order for opaque types without boilerplate.

```scala
import com.github.chenharryhua.nanjin.common.OpaqueLift

// Given Show[String], derive Show[MyOpaqueString]
given Show[MyOpaqueString] = OpaqueLift.lift[MyOpaqueString, String, Show]
```

Drawbacks:
- Uses `asInstanceOf` — type-safe by construction (opaque types erase to their representation) but bypasses the compiler's type checker.

### UpdateConfig / EnableConfig / HasProperties

Mixin traits for configuration builder patterns. `UpdateConfig[A, B]` provides `updateConfig(f: Endo[A]): B`. `HasProperties` exposes a `Map[String, String]` for Java client configuration.

Drawbacks:
- Pure interfaces with no logic — they add consistency but no enforcement. Implementations must honor the contract manually.

---

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
