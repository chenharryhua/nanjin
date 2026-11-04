# Date and time utilities

The datetime module provides parsing, bounded and unbounded time ranges, recurring local-time
windows, calendar-period parsing, precision helpers, and Cats/Circe instances for Java and SQL date
types.

All range boundaries use half-open semantics: the start is included and the end is excluded.

## Contents

- [Dependency](#dependency)
- [Date-time parsing](#date-time-parsing)
  - [Built-in parsers](#built-in-parsers)
  - [Combine parsers](#combine-parsers)
- [DateTimeRange](#datetimerange)
  - [Construct a range](#construct-a-range)
  - [Accepted boundary types](#accepted-boundary-types)
  - [Membership and open bounds](#membership-and-open-bounds)
  - [Calendar-day helpers](#calendar-day-helpers)
  - [Days and subranges](#days-and-subranges)
  - [Durations and ordering](#durations-and-ordering)
- [LocalTimeRange](#localtimerange)
- [Period parsing](#period-parsing)
- [Resolution and distance helpers](#resolution-and-distance-helpers)
- [Typeclass instances and JSON](#typeclass-instances-and-json)
- [Wire format and time-zone guidance](#wire-format-and-time-zone-guidance)
- [Source map and validation](#source-map-and-validation)

## Dependency

```scala
libraryDependencies += "com.github.chenharryhua" %% "nj-datetime" % "<version>"
```

The module uses `java.time` as its primary model. `java.sql.Date` and `java.sql.Timestamp` are
supported through additional Cats and Circe instances; the module does not provide JDBC mappings.

## Date-time parsing

`DateTimeParser[A]` parses a string without throwing:

```scala
import com.github.chenharryhua.nanjin.datetime.DateTimeParser

import java.time.Instant

val parsed: Either[com.github.chenharryhua.nanjin.datetime.FailedParsers, Instant] =
  DateTimeParser[Instant].parse("2026-01-02T03:04:05.123456789Z")
```

A failure contains the names of all parsers that were attempted. Convert it to a standard Java
`DateTimeParseException` when an exception-based boundary is required:

```scala
val instant =
  DateTimeParser[Instant]
    .parse(input)
    .left
    .map(_.parseException(input))
```

The parsers delegate to the corresponding Java `parse` methods. Inputs therefore use Java's
ISO-8601 forms and are not trimmed before parsing.

### Built-in parsers

| Result type | Example |
| --- | --- |
| `LocalDate` | `2026-01-02` |
| `LocalTime` | `03:04:05.123` |
| `LocalDateTime` | `2026-01-02T03:04:05` |
| `Instant` | `2026-01-02T03:04:05Z` |
| `OffsetDateTime` | `2026-01-02T03:04:05+11:00` |
| `ZonedDateTime` | `2026-01-02T03:04:05+11:00[Australia/Sydney]` |

Each parser reports its type name on failure, such as `FailedParsers("Instant")`.

### Combine parsers

`DateTimeParser` has a Cats `Alternative` instance. `combineK`, or `<+>`, tries parsers from left
to right and accumulates parser names when all alternatives fail:

```scala
import cats.syntax.all.*

import java.time.{Instant, OffsetDateTime, ZonedDateTime}

val instantLike: DateTimeParser[Instant] =
  DateTimeParser[Instant] <+>
    DateTimeParser[OffsetDateTime].map(_.toInstant) <+>
    DateTimeParser[ZonedDateTime].map(_.toInstant)

val result = instantLike.parse("2026-01-02T14:04:05+11:00")
```

The first successful parser wins. `Alternative.empty` always fails with `EmptyParser`, while
`Alternative.pure` ignores its input and returns the supplied value.

## DateTimeRange

`DateTimeRange` stores optional `Instant` bounds and one fixed `ZoneId`. Inputs without an offset or
zone are resolved in that zone when the setter is called.

### Construct a range

```scala
import com.github.chenharryhua.nanjin.datetime.DateTimeRange

import java.time.ZoneId

val zone = ZoneId.of("Australia/Sydney")

val range =
  DateTimeRange(zone)
    .withStartTime("2026-01-01T09:00:00")
    .withEndTime("2026-01-01T17:00:00")
```

`DateTimeRange(zone)` starts with both bounds unset. The fluent API has no zone setter; create a
new range to use another zone. `DateTimeRange(tick)` copies a Nanjin `Tick`'s `commence`, `conclude`,
and zone.

### Accepted boundary types

Both `withStartTime` and `withEndTime` accept:

| Input | Interpretation |
| --- | --- |
| `Instant` | Stored directly |
| `OffsetDateTime` | Converted to its instant |
| `ZonedDateTime` | Converted to its instant; displayed later in the range's zone |
| `LocalDateTime` | Interpreted in the range's zone |
| `LocalDate` | Interpreted as midnight in the range's zone |
| `LocalTime` | Anchored to today in the range's zone at call time |
| `java.sql.Timestamp` | Converted to its instant |
| `Long` | Epoch milliseconds |
| `String` | Parsed as one of the supported ISO date/time representations |

The string setter tries `Instant`, `OffsetDateTime`, `ZonedDateTime`, `LocalDate`, `LocalTime`, and
`LocalDateTime`. Zoned and offset values retain their instant, while local values use the range zone.
Malformed strings throw `DateTimeParseException` listing every attempted parser.

`withTimeRange(start, end)` sets both bounds from strings. Setter order does not matter because each
input is resolved to an instant immediately.

### Membership and open bounds

`inBetween` uses `[start, end)`:

```scala
import java.time.Instant

val start = Instant.parse("2026-01-01T00:00:00Z")
val end = Instant.parse("2026-01-02T00:00:00Z")
val utcDay = DateTimeRange(ZoneId.of("UTC")).withStartTime(start).withEndTime(end)

utcDay.inBetween(start) // true
utcDay.inBetween(end)   // false
```

| Bounds | Membership |
| --- | --- |
| Both set | `start <= value < end` |
| Start only | `start <= value` |
| End only | `value < end` |
| Neither set | Every instant is included |

Construction does not reject an end before the start. Such a range retains its signed duration but
contains no instant under the bounded membership rule. Validate ordering at an application boundary
when reversed input is invalid.

### Calendar-day helpers

```scala
import java.time.LocalDate

val oneDay = DateTimeRange(zone).withOneDay(LocalDate.of(2026, 1, 2))
val today = DateTimeRange(zone).withToday
val yesterday = DateTimeRange(zone).withYesterday
val ereyesterday = DateTimeRange(zone).withEreyesterday
val recent = DateTimeRange(zone).withLastSeconds(300)
```

`withOneDay` covers local midnight through the next local midnight as a half-open range. The string
overload accepts a `LocalDate`. `LocalDate.MAX` uses `LocalDateTime.MAX` for the end instead of
calling `plusDays(1)`.

`withToday`, `withYesterday`, `withEreyesterday`, and `withLastSeconds` read the current clock when
called and use the range's zone. `withEreyesterday` is the public method name for the day before
yesterday. A negative value passed to `withLastSeconds` creates a reversed range; it is not rejected.
`withLastSeconds` subtracts on the zone's local timeline before resolving both bounds to instants, so
an interval crossing a daylight-saving transition is not guaranteed to have that many elapsed
seconds.

### Days and subranges

`days` returns the local calendar dates intersected by a finite, forward range:

```scala
val days: List[LocalDate] =
  DateTimeRange(zone)
    .withStartTime("2026-01-01T23:00:00")
    .withEndTime("2026-01-03T01:00:00")
    .days
    .toList
```

The end remains exclusive. A bound exactly at midnight does not include the following day. An
unbounded, zero-width, or reversed range returns an empty `LazyList`.

`subranges` splits a finite range into consecutive elapsed-time windows and clips the final window:

```scala
import scala.concurrent.duration.*

val windows: LazyList[DateTimeRange] =
  range.subranges(15.minutes)
```

The interval must resolve to at least one millisecond. Bounds and interval stepping use epoch
milliseconds, so sub-millisecond precision is not preserved in generated subranges. Unbounded or
reversed ranges produce no subranges.

### Durations and ordering

- `javaDuration` returns the signed `java.time.Duration` between finite bounds.
- `finiteDuration` converts that elapsed duration to Scala `FiniteDuration`.
- `period` returns the calendar `Period` between the bounds' local dates.
- `zonedStartTime` and `zonedEndTime` display stored instants in the fixed zone.

Calendar periods and elapsed durations answer different questions. Across a daylight-saving
transition, one local day can be 23 or 25 elapsed hours.

The Cats `PartialOrder[DateTimeRange]` compares containment by stored bounds. A range is greater than
another when it contains it. Open bounds are infinite, crossing ranges that contain neither other
range are incomparable, and the display zone is not part of the containment comparison.

## LocalTimeRange

`LocalTimeRange` models a recurring wall-clock window independently of a date:

```scala
import com.github.chenharryhua.nanjin.datetime.LocalTimeRange

import java.time.LocalTime
import scala.concurrent.duration.*

val overnight = LocalTimeRange(LocalTime.of(22, 0), 8.hours)

overnight.inBetween(LocalTime.of(23, 0)) // true
overnight.inBetween(LocalTime.of(5, 59)) // true
overnight.inBetween(LocalTime.of(6, 0))  // false
```

The start is inclusive and the computed end is exclusive. A range whose end is earlier than its
start crosses midnight and matches with OR logic. Other duration rules:

| Duration | Result |
| --- | --- |
| Negative | Matches no local time |
| Zero | Matches no local time |
| Between zero and 24 hours | Matches the half-open wall-clock interval, possibly across midnight |
| At least 24 hours | Matches every local time |

The `ZonedDateTime` overload uses only `toLocalTime`; it ignores the date and zone. This type models a
recurring clock schedule, not an elapsed-time interval across a daylight-saving transition.

## Period parsing

`period` parses a `java.time.Period` into `Validated[NonEmptyList[String], Period]` and trims
surrounding whitespace:

```scala
import com.github.chenharryhua.nanjin.datetime.period

val iso = period("P1Y2M3D")
val words = period("1 year 2 months 3 days")
```

ISO-8601 parsing is attempted first and supports all forms accepted by `Period.parse`, including
weeks and signed components. The fallback grammar accepts non-negative integer fields in this order:

1. years
2. months
3. days

Any one field or ordered pair is valid, units are case-insensitive, singular and plural are accepted,
and whitespace between numbers and units is optional. Examples include `7years`, `2 Months 3 days`,
and `1 year 3 days`.

The fallback grammar rejects decimals, signs, words instead of digits, repeated units, out-of-order
units, and trailing input. Counts must fit a Java `Int`. Grammar failures are returned as validation
data; numeric overflow during the homebrew parser's integer conversion can still throw, so bound
untrusted input before parsing.

## Resolution and distance helpers

```scala
import com.github.chenharryhua.nanjin.datetime.*

import java.time.{LocalDateTime, LocalTime}

val timestamp = LocalDateTime.of(2026, 1, 2, 3, 4, 5, 678000000)

dayResolution(timestamp)    // 2026-01-02
hourResolution(timestamp)   // 2026-01-02T03:00
minuteResolution(timestamp) // 2026-01-02T03:04
secondResolution(timestamp) // 2026-01-02T03:04:05

distance(LocalTime.of(23, 0), LocalTime.of(1, 0)) // 2 hours
```

`distance(start, end)` moves forward on a 24-hour clock. If `end` is earlier, it wraps through
midnight; equal values have zero distance. Nanosecond precision is retained.

`toLocalDateTime(LocalDate)` uses midnight. `toLocalDateTime(LocalTime)` anchors the time to
`LocalDate.now()` in the system default zone, unlike `DateTimeRange`'s local-time setter, which uses
the range zone.

Constants are provided for `oneMillisecond`, `oneSecond`, `oneMinute`, `oneHour`, and `oneDay` as
Scala `FiniteDuration` values.

## Typeclass instances and JSON

Import the module instances when working directly with SQL dates or `FiniteDuration`:

```scala
import com.github.chenharryhua.nanjin.datetime.instances.given
```

`DateTimeInstances` extends cats-time's complete Java-time instance set and adds:

| Type | Added instances | JSON representation |
| --- | --- | --- |
| `java.sql.Timestamp` | `Hash`, `Order`, `Show`, `Encoder`, `Decoder` | ISO-8601 instant string, preserving nanoseconds |
| `java.sql.Date` | `Hash`, `Order`, `Show`, `Encoder`, `Decoder` | ISO local-date string |
| `FiniteDuration` | `Encoder`, `Decoder` | ISO-8601 Java duration string |

A `FiniteDuration` JSON value that exceeds Scala's finite nanosecond range fails decoding instead of
throwing an arithmetic exception.

`DateTimeRange` has a custom Circe codec:

```json
{
  "zone_id": "UTC",
  "start": "2026-01-02T03:04:05",
  "end": null
}
```

Bounds are local date-time strings in the declared zone, or JSON null when open. `LocalTimeRange`
derives an object codec with fields `start` and `duration`; the duration uses ISO-8601 text such as
`PT8H`.

## Wire format and time-zone guidance

The following serialized forms are externally observable and should remain stable:

| Value | Serialized form |
| --- | --- |
| `DateTimeRange` | Object keys `zone_id`, `start`, and `end` |
| `LocalTimeRange` | Object keys `start` and `duration` |
| SQL `Timestamp` | ISO instant string |
| SQL `Date` | ISO local-date string |
| `FiniteDuration` | ISO Java-duration string |

`DateTimeRange` JSON stores local date-times plus a zone id, not the original instant or offset. This
normally reconstructs the same instant. During a daylight-saving overlap, one local time has two
valid offsets; decoding uses Java's default `LocalDateTime.atZone` resolution and may choose a
different occurrence than the original instant. Store an explicit instant separately when overlap
identity must be exact.

Other time-zone rules:

- Offset and zoned inputs are converted to instants before storage, then displayed in the range zone.
- Local date-time inputs use the range zone's gap and overlap resolution rules.
- A calendar day follows local midnights and is not guaranteed to contain 24 elapsed hours.
- Epoch `Long` setters use milliseconds.
- Generated subranges have millisecond resolution even when original instants contain nanoseconds.
- `LocalTimeRange` is date-free and does not model daylight-saving gaps or overlaps.

Half-open `[start, end)` ranges compose without double-counting a boundary: the end of one subrange
can be the start of the next.

## Source map and validation

| Area | Source |
| --- | --- |
| Parser typeclass | [`DateTimeParser.scala`](../src/main/scala/com/github/chenharryhua/nanjin/datetime/DateTimeParser.scala) |
| Date-time ranges | [`DateTimeRange.scala`](../src/main/scala/com/github/chenharryhua/nanjin/datetime/DateTimeRange.scala) |
| Local-time windows | [`LocalTimeRange.scala`](../src/main/scala/com/github/chenharryhua/nanjin/datetime/LocalTimeRange.scala) |
| Period parser | [`period.scala`](../src/main/scala/com/github/chenharryhua/nanjin/datetime/period.scala) |
| Resolution and distance helpers | [`datetime.scala`](../src/main/scala/com/github/chenharryhua/nanjin/datetime/datetime.scala) |
| Cats and Circe instances | [`DateTimeInstances.scala`](../src/main/scala/com/github/chenharryhua/nanjin/datetime/DateTimeInstances.scala) |

From the repository root:

```shell
sbt 'datetime / Test / compile'
sbt 'datetime / test'
```

The test suite is deterministic and does not require external services.
