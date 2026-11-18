# Splunk observer

`nj-observer-splunk` provides `SplunkObserver`, which translates Guard events to JSON and posts them
to Splunk's HTTP Event Collector (HEC). The observer wraps each translated event in a HEC envelope
with its timestamp and optional index, source, and sourcetype metadata. Events pass through
unchanged.
Examples assume an existing Guard event stream named `events` and a configured HTTP client.

```scala
import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.observers.splunk.{HecConfig, SplunkObserver}
import org.http4s.Uri
import org.http4s.client.Client

val config = HecConfig(
  endpoint = hecEndpoint,
  token = hecToken,
  index = Some("main"),
  source = Some("nanjin"),
  sourceType = Some("_json")
)

val observer = SplunkObserver[IO](httpClient)
  .withTranslator(_.skipMetricsSnapshot)

val observed = events.through(observer.observe(config))
```

`httpClient` is a `Resource[IO, Client[IO]]`; `hecEndpoint` is the HEC event endpoint as a `Uri`.
Supply `hecToken` from a secret store rather than embedding a real token in source. The default
translator is `PrettyJsonTranslator`; use `withTranslator` to filter or customize events. A failed
HTTP request is swallowed so it does not terminate the event stream.

## Artifact

```scala
"com.github.chenharryhua" %% "nj-observer-splunk" % version
```
