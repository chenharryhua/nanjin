# PostgreSQL observer

`nj-observer-database` provides `PostgresObserver`, which translates Guard events to pretty JSON and
inserts one JSON value per event into a PostgreSQL table using a Skunk `Session`. The FS2 pipe is a
side-effecting tap: events pass through unchanged. Examples assume an existing Guard event stream
named `events`.

## Setup

Create a table with a JSON column and a defaulted timestamp column:

```sql
CREATE TABLE public.event_stream (
  info json NULL,
  "timestamp" timestamptz NULL DEFAULT CURRENT_TIMESTAMP
);
```

The observer inserts the translated JSON into the table named in `observe`. The timestamp column
uses its database default. Pass a managed Skunk session resource:

```scala
import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.observers.postgres.PostgresObserver
import skunk.Session

val observer = PostgresObserver[IO](postgresSession)
  .withTranslator(_.skipMetricsSnapshot)

val observed =
  events.through(observer.observe("public.event_stream"))
```

`postgresSession` is a `Resource[IO, Session[IO]]`. The default translator is
`PrettyJsonTranslator`; use `withTranslator` to filter event kinds or customize the JSON.
Failures inserting incoming events are logged and swallowed, so they do not interrupt stream
processing. On stream finalization, a synthesized `ServiceStop(ByCancellation)` is inserted for
each service still running, if the translator accepts it.

## Artifact

```scala
"com.github.chenharryhua" %% "nj-observer-database" % version
```
