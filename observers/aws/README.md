# AWS observers

`nj-observer-aws` provides Guard observers for Amazon SNS, SQS, SES, and CloudWatch. The event
observers consume a Guard `Stream[F, Event]` as an FS2 pipe and pass the original events downstream.
`CloudWatchObserver` is different: it exports `MeteredCounts` produced by the metrics API.

The AWS client resources come from `nj-aws`; configure their region and credentials using the AWS SDK
provider chain or your application's preferred credential source. See the [Guard event-system
guide](../../guard/doc/README.md#event-system) for the event model and `EventPipe` filters.
Examples assume an existing Guard event stream named `events`; client resources and endpoint values
are supplied by the application.

## SNS

`SnsObserver` translates each event to pretty JSON and publishes it immediately to a topic. It uses
`PrettyJsonTranslator` by default; change or filter the output with `withTranslator`.

```scala
import cats.effect.IO
import com.github.chenharryhua.nanjin.aws.{SimpleNotificationService, SnsArn}
import com.github.chenharryhua.nanjin.guard.event.EventPipe
import com.github.chenharryhua.nanjin.guard.observers.sns.SnsObserver

val sns = SnsObserver[IO](snsClient)
  .withTranslator(_.filter(EventPipe.logLevel(_.Warn)))

val observed = events.through(sns.observe(SnsArn("arn:aws:sns:region:account:topic")))
```

`snsClient` is a `Resource[IO, SimpleNotificationService[IO]]`. On stream finalization, a
`ServiceStop(ByCancellation)` is published for each service still running. A failed publish is
swallowed so it does not stop the event stream.

## SQS

`SqsObserver` sends each event as a JSON message. Its default identity translator serializes the
event unchanged; `withTranslator` can filter or reshape events before sending.

```scala
import cats.effect.IO
import com.github.chenharryhua.nanjin.aws.SqsUrl
import com.github.chenharryhua.nanjin.guard.observers.sqs.SqsObserver
import software.amazon.awssdk.services.sqs.model.SendMessageRequest

val sqs = SqsObserver[IO](sqsClient)
val queue = SendMessageRequest.builder().queueUrl(queueUrl)
val observed = events.through(sqs.observe(queue))
```

`sqsClient` is a `Resource[IO, SimpleQueueService[IO]]`. For a FIFO queue, use
`observe(SqsUrl.Fifo(url), messageGroupId)`; a single message group preserves event order. A random
deduplication ID is assigned to each message. As with SNS, finalization reports any still-running
services with a synthesized stop event, and send failures are swallowed.

## SES email

`EmailObserver` renders translated events as HTML and batches them into digest emails. It flushes when
the buffer reaches its capacity, on each scheduled policy tick, and when the input stream ends. The
final flush includes a synthesized cancellation stop for any service still running. Scheduled flushes
default to off; exhausting the flush policy only disables future scheduled flushes. Empty flushes are
sent as heartbeats.

```scala
import cats.data.NonEmptyList
import cats.effect.IO
import com.github.chenharryhua.nanjin.aws.{Email, SimpleEmailService}
import com.github.chenharryhua.nanjin.guard.observers.ses.EmailObserver
import scala.concurrent.duration.*
import java.time.ZoneId

val digest = EmailObserver[IO](sesClient)
  .withCapacity(200)
  .withPolicy(_.fixedDelay(12.hours).repeat)
  .withZoneId(ZoneId.of("Australia/Sydney"))

val observed =
  events.through(digest.observe(Email("sender@example.com"),
    NonEmptyList.one(Email("ops@example.com")), "Service events"))
```

`sesClient` is a `Resource[IO, SimpleEmailService[IO]]`. Capacities below five are raised to five.
The default email translator is HTML; use `withTranslator` to customize it, and `withOldestFirst` to
change the default newest-first ordering. SES send failures are swallowed.

## CloudWatch metrics

`CloudWatchObserver.scrape` consumes `MeteredCounts`, not `Event`s. `agent.adhoc.meteredCounts`
provides a stream of meter and timer deltas with a policy-defined cadence. Each count becomes a
CloudWatch metric datum with service, domain, and metric-label dimensions. Requests are batched up to
1000 data points or the configured interval.

```scala
import cats.effect.IO
import com.github.chenharryhua.nanjin.aws.CloudWatch
import com.github.chenharryhua.nanjin.guard.observers.cloudwatch.CloudWatchObserver
import software.amazon.awssdk.regions.Region

val cloudWatch =
  CloudWatchObserver[IO](CloudWatch[IO](_.region(Region.AP_SOUTHEAST_2)))

val export = service.eventStreamS { agent =>
  agent.adhoc.meteredCounts(_.crontab(_.minutely))
    .through(cloudWatch.scrape("MyApp/Metrics"))
}
```

`scrape` defaults to standard 60-second storage resolution and a 15-second maximum buffering
interval. CloudWatch publish failures are swallowed. See
[the example observers](../../example/src/main/scala/example/observers.scala) for construction
examples.

## Artifact

```scala
"com.github.chenharryhua" %% "nj-observer-aws" % version
```
