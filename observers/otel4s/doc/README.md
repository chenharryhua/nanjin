# OpenTelemetry logs observer

`nj-observer-otel4s` provides `Otel4sObserver`, which translates Guard events to structured JSON
log bodies and emits them through an otel4s `LoggerProvider`. Event levels are mapped to
OpenTelemetry severities (`Good` maps to `Info`), and the event timestamp is used as the log
timestamp. Events pass through unchanged for other stream consumers. Examples assume an existing
Guard event stream named `events`.

```scala
import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.observers.otel4s.Otel4sObserver
import com.github.chenharryhua.nanjin.guard.event.Event
import fs2.{Pipe, Stream}
import org.typelevel.otel4s.logs.LoggerProvider

def observeEvents[Ctx](provider: LoggerProvider[IO, Ctx]): Pipe[IO, Event, Event] =
  Otel4sObserver[IO, Ctx](provider)
    .withTranslator(_.skipMetricsSnapshot)
    .observe("my-service")

val observed = events.through(observeEvents(loggerProvider))
```

The default translator is `PrettyJsonTranslator`. Use `withTranslator` to skip event types or
customize the JSON body. Configure the supplied provider and its exporters with otel4s; this observer
does not create or own the provider.

## Artifact

```scala
"com.github.chenharryhua" %% "nj-observer-otel4s" % version
```
