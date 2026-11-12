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

// Take whichever of two policies proposes the shorter snooze; exhausted when either is
val p7 = Policy.crontab(_.hourly).repeat.meet(Policy.fixedDelay(10.minutes).repeat)

// Pass a policy value where a builder function `Policy.type => Policy` is expected
val prepared = Policy.fixedDelay(1.second).repeat
val p8 = Policy.fresh(prepared)

// Exclusions and offsets
val p5 = Policy.crontab(_.hourly).except(localTimes.noon).offset(100.millis)

// An exclusion that can never be satisfied yields an empty policy rather than
// firing at the excluded time anyway
val p6 = Policy.crontab(_.daily.midnight).repeat.except(localTimes.midnight) // never ticks
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
  cb.state                  // inspect current state
}
```

Drawbacks:
- `maxFailures` counts failures in Closed state only. A success resets the counter — there's no sliding window or rate-based opening.
- The breaker is scoped to a `Resource` — it can't outlive the resource's lifetime. For long-lived breakers, the enclosing resource must also be long-lived.
- The policy is a free-running cadence started when the breaker is acquired, not a timer started when it opens. Each tick promotes an open breaker to half-open, so the open period lasts only until the next tick: with `_.fixedDelay(10.seconds)` a breaker that opens one second before a tick probes one second later, not ten. Treat the cadence as the longest acceptable wait, anywhere in `(0, cadence]`, rather than a guaranteed cool-down.
- Failure counting is per-call: concurrent failures each advance the counter, so `maxFailures` calls failing in parallel trip the breaker just as that many successive failures do. A success resets the count only when no other call changed it while that success was running, so a failure overlapping a success is never erased. Half-open admits exactly one probe at a time (single-flight).

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

A decision function that *fails* instead of returning a transition stops the retry and raises
`Retry.DecisionFailure`, which carries both errors and modifies neither:

```scala
retry(action).recover {
  case ex: Retry.DecisionFailure =>
    ex.decisionFailure   // what the decision function raised; also ex.getCause
    ex.operationFailure  // what the retried operation raised
}
```

Drawbacks:
- Only retries on `Throwable`. For retrying on "bad" successful values (e.g., HTTP 503 response), wrap in an exception or use a domain-specific retry (see `httpRetry`).
- The `Retry[F]` instance is reusable, but `elapsed` tracks time within a single `retry(fa)` call — not across multiple calls to the same instance.
- `retryAfter` overrides the current sleep but doesn't change the policy progression. The next attempt still uses the policy's next scheduled delay.
- A failing decision function replaces the operation's exception with `DecisionFailure`, so code matching on the operation's exception type must unwrap it. Return `giveUp` to stop retrying and propagate the operation failure unchanged.
- `DecisionFailure` attaches nothing to either original throwable, so logs show the operation failure's class and message (from the `DecisionFailure` message) but not its stack trace. Read `operationFailure` when you need the trace.

### SingleFlight

Deduplicates concurrent effectful calls. Each `SingleFlight` instance represents one implicit key: at most one
submitted effect runs at a time, and concurrent callers normally receive that flight's result without evaluating
their own effects.

```scala
import com.github.chenharryhua.nanjin.common.resilience.SingleFlight

SingleFlight[IO, Result].flatMap { sf =>
  sf(expensiveComputation) // starts or joins a flight
  sf.tryApply(computation) // returns None without evaluating computation if a flight exists
}
```

Drawbacks:
- Callers sharing an instance must submit logically equivalent operations.
- All followers receive the same result, including the same error when the worker fails.
- Canceling one caller does not cancel shared work while other waiters remain.
- Canceling the last waiter cancels the worker and waits for termination, which can remain pending if the worker
  is indefinitely uncancelable.
- There is no TTL or cache; each call after a flight completes can start a new computation.

---

## logging — Structured Logging

### Log[F]

Abstract effectful logger with level-based filtering and JSON-encoded messages. Implementations supply the SPI (`create`/`publish`/`enabled`); `emit` is the single public primitive and the named helpers (`error`/`warn`/`good`/`info`/`debug`) each pin a level and delegate to it.

```scala
import com.github.chenharryhua.nanjin.common.logging.{Log, LogEntry, LogLevel}

// Levels: Error > Warn > Good > Info > Debug
log.error("critical failure", exception)
log.warn("degraded state")
log.good("operation succeeded")
log.info("status update")
log.debug(expensiveDebugInfo)  // by-name: only forced when Debug is enabled

// emit — for a dynamic level, or a pre-built record
log.emit("status update", LogLevel.Info, None)
log.emit(LogEntry("rejected", LogLevel.Warn, Some(exception)))
```

`LogEntry[S](message, level, cause)` is a self-contained record and derives `Functor`, so `entry.map(_.render)` reshapes the payload while keeping its level and cause.

`Log.noop[F]` is a logger with every level disabled, for tests and for switching logging off.

Drawbacks:
- Messages must have a `circe.Encoder` instance. Plain strings work (String has an Encoder), but custom types need deriving.
- `good` is a non-standard log level — sits between Warn and Info. External log aggregators won't recognize it natively.
- `emit` swallows every failure on its path — the level check, encoding, and publishing — so logging never disrupts the surrounding effect. A broken logger is therefore silent rather than loud; a record whose level could not be determined is simply not emitted.

### LogLevel

Enum with `Error`, `Warn`, `Good`, `Info`, `Debug`. Provides circe codecs, Show, and Order instances. `Good` is specific to nanjin — represents "success worth noting" between Warn and Info severity. Each level maps onto an `org.slf4j.event.Level`; `Good` maps to INFO like `Info`, but stays distinct so a console renderer can colour it differently.

### LogLink / LogLocator

`LogLink` is an opaque `String` holding a fully-formed URL into a log viewer; read it with `.value`. It carries circe codecs so a link can travel on an event (see guard's event model), where it appears as a plain JSON string.

`LogLocator` turns an `Instant` into a `LogLink`, letting the core obtain a deep link to the logs around an event while a backend owns the URL construction and any time window placed around the timestamp. A locator is built once per running service and reused.

```scala
import com.github.chenharryhua.nanjin.common.logging.{LogLink, LogLocator}
import java.time.Instant

val locator: LogLocator = (timestamp: Instant) =>
  LogLink(s"https://logs.example.com/?at=$timestamp")

locator.locate(Instant.now()).value // the URL
```

See the aws module's `CloudWatchLogLocator` for the CloudWatch console implementation.

---

## security — Keeping Secrets Out of Logs

Two complementary tools stop sensitive data from leaking into logs, exception messages, and on-screen output: `Secret` prevents leaks at the type level, while `json.redact` scrubs values at display time.

### Secret

A wrapper for a sensitive value (password, client secret, authorization code, token, ...) whose contents are masked in every rendered form.

```scala
import com.github.chenharryhua.nanjin.common.Secret

final case class DbConfig(user: String, password: Secret)

val cfg = DbConfig("admin", Secret("hunter2"))

cfg.toString        // DbConfig(admin,***) — the enclosing case class masks it too
cfg.password.value  // "hunter2" — the single, intentional cleartext escape hatch
```

Why it holds where a plain `String` (or an `opaque type`) would leak:
- `toString` and `Show` render `***`, never the value.
- It is a real runtime class, not an `opaque type` — an opaque type erases to `String`, so a case class embedding it would print the underlying value in its default `toString`. The class masks even when nested inside other structures.
- The JSON `Encoder` is a **masking** encoder that emits `"***"`. An enclosing type can therefore auto-derive a codec for logging, and the secret still never reaches JSON in cleartext. There is deliberately **no** `Decoder` — a secret cannot be recovered from its mask, so a masked value can't round-trip back into a real one.
- `equals`/`hashCode` are value-based, so `Secret` still works as a map key. `equals` compares all code units without early exit, so its runtime does not reveal the length of the matching prefix the way `String.equals` does, and the `Eq[Secret]` instance delegates to it — compare with `===` rather than unwrapping both sides with `.value`.

The raw value is reachable only through `.value`, used at trust boundaries (building a JDBC config, a token request, ...).

```scala
import io.circe.syntax.*
Secret("hunter2").asJson              // "***"
DbConfig("admin", Secret("hunter2")).asJson  // {"user":"admin","password":"***"}
```

Drawbacks:
- `.value` is an unguarded escape hatch — once called, the plain `String` is subject to the usual leak paths. Keep the unwrap as close to the boundary as possible.
- The encoder masks on **every** serialization path, including wire payloads. A request that needs the real secret must take it from `.value`; serializing the enclosing type to a wire body will send `"***"`, not the secret.
- `hashCode` is the underlying `String`'s, so it exposes a 32-bit fingerprint of the secret wherever a hash is visible (including an enclosing case class's `hashCode`). That is the price of working as a map key.

### json — Display-Only Transforms

Transforms for **human-facing output only** (logs, dashboard). They rewrite values for readability and safety, so the result is **not** machine-readable and must never be used as wire format or fed back into a decoder.

```scala
import com.github.chenharryhua.nanjin.common.json

// redact: replace sensitive string fields (matched by key, at any depth) with a marker
val scrub = json.redact("password", "client_secret")
scrub(payload)  // matching string fields become "redacted(*****)"

// redact with a custom function
json.redact(List("token"), _.take(4) + "…")(payload)

// prettify: group numbers and humanize durations for display
json.prettify(event)  // 1234567 -> "1,234,567", duration -> "1 minute 5 seconds"
```

Drawbacks:
- `redact` matches on **field name** and rewrites **string** values only. A sensitive value nested under a matching key as an object/array/number is left structurally intact (recursion still catches sensitive string leaves inside it) — it does not collapse a whole container into a marker.
- Both transforms are lossy and display-only. Round-tripping the output through a decoder will not reproduce the original.

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

`create` takes the maximum number of units to show, largest first, and requires a positive value; seven units are available (day down to nano).

Drawbacks:
- English-only output. No i18n support.
- Uses singular/plural rules (`1 day` vs `2 days`) — not suitable for machine parsing.
- A non-finite Scala duration (`Duration.Inf`, ...) is rendered via its own `toString` instead of a breakdown. The `Instant`/`ZonedDateTime`/`JavaDuration`/`Time` overloads convert to a Scala duration first, which holds at most ~292 years, so a wider span throws `IllegalArgumentException` rather than formatting — reachable only with sentinels such as `Instant.MAX`.

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

### FieldNames

Case-class field names, extracted at compile time from the product mirror in declaration order.

```scala
import com.github.chenharryhua.nanjin.common.FieldNames

final case class Tiger(name: String, age: Int, colour: String)

FieldNames.of[Tiger]  // List("name", "age", "colour")
```

Drawbacks:
- Requires a `Mirror.ProductOf[A]` — works for case classes and product types only, not arbitrary types.
- Returns the declared Scala field names, not any renamed JSON keys (e.g. a circe `@JsonKey` or custom codec mapping is not reflected).

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
- cats-time (java.time instances), kittens (typeclass derivation)
- Droste (recursion schemes)
- Monocle (optics), circe-optics (`Plated` for `Json`)
- Circe (JSON)
- Cron4s (cron expressions)
- Squants (units of measure)
- Chimney (transformations)
- Apache commons-lang3 (exception rendering)

## Artifact

```
"com.github.chenharryhua" %% "nj-common" % version
```
