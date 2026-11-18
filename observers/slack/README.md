# Slack observer

`nj-observer-slack` provides `SlackObserver`, which renders Guard events as Slack Block Kit messages
and posts them to an incoming webhook. Its FS2 pipe passes events through unchanged.
Examples assume an existing Guard event stream named `events` and a configured HTTP client.

```scala
import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.observers.slack.SlackObserver
import org.http4s.Uri
import org.http4s.client.Client

val observer = SlackObserver[IO](httpClient)
  .withMaxStackTraceFrames(20)
  .withIconUrl(iconUri)

val observed = events.through(observer.observe(webhook))
```

`httpClient` is a `Resource[IO, Client[IO]]`; `webhook` and `iconUri` are `Uri` values. The default
translator builds the Slack message. Use `withTranslator` to skip event kinds or customize it.
Stack traces can be truncated with `withMaxStackTraceFrames`. Posts carry an idempotency key to
allow Slack to deduplicate retries. Failed posts are swallowed. On finalization, a stop event is
rendered for each service still running. Keep webhook URLs and other credentials in a secret store.

## Artifact

```scala
"com.github.chenharryhua" %% "nj-observer-slack" % version
```
