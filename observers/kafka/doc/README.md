# Kafka observer

`nj-observer-kafka` provides `KafkaObserver`, an FS2 pipe that produces Guard events to a Kafka
topic as JSON key/value records. Events pass through unchanged, so the pipe can be composed with
other stream consumers. Examples assume an existing Guard event stream named `events`.

```scala
import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.observers.kafka.KafkaObserver
import com.github.chenharryhua.nanjin.kafka.KafkaContext

val observer = KafkaObserver[IO](kafkaContext)
  .withTranslator(_.skipMetricsSnapshot)

val observed = events.through(observer.observe("service-events"))
```

`kafkaContext` is a configured `KafkaContext[IO]`. The default identity translator serializes the
event as the JSON value. The JSON key contains the task and service names. Use `withTranslator` to
filter or transform events before producing them. On stream finalization, a stop event is produced
for each service still running. Per-event producer failures are logged and swallowed; a failure
while producing a synthesized final stop can still fail stream finalization.

See the [Kafka module](../../../kafka/doc/README.md) for producer configuration and the
[Guard event-system guide](../../../guard/doc/README.md#event-system) for event types and filtering.

## Artifact

```scala
"com.github.chenharryhua" %% "nj-observer-kafka" % version
```
