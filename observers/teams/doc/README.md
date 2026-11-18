# Microsoft Teams observer

`nj-observer-teams` provides `TeamsObserver`, which renders Guard events as Adaptive Cards and posts
them to a Teams incoming webhook. Its FS2 pipe passes events through unchanged.
Examples assume an existing Guard event stream named `events` and a configured HTTP client.

```scala
import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.observers.teams.TeamsObserver
import org.http4s.Uri
import org.http4s.client.Client

val observer = TeamsObserver[IO](httpClient)
  .withMaxStackTraceFrames(20)
  .withTranslator(_.skipMetricsSnapshot)

val observed = events.through(observer.observe(webhook))
```

`httpClient` is a `Resource[IO, Client[IO]]`; `webhook` is the incoming webhook `Uri`. The default
translator creates the Adaptive Card. Use `withTranslator` to skip event types or customize cards,
and `withMaxStackTraceFrames` to limit rendered traces. Failed posts are swallowed. On finalization,
a stop event is posted for each service still running. Keep webhook URLs in a secret store.

## Artifact

```scala
"com.github.chenharryhua" %% "nj-observer-teams" % version
```
