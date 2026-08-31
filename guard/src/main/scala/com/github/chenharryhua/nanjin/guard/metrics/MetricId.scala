package com.github.chenharryhua.nanjin.guard.metrics

import cats.derived.derived
import cats.effect.Unique
import cats.effect.kernel.Clock
import cats.kernel.Eq
import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import cats.{Applicative, Hash}
import com.github.chenharryhua.nanjin.guard.config.{Domain, Service, Task}
import io.circe.{Codec, Decoder, Encoder}
import org.typelevel.otel4s.Attribute
import squants.{Each, Quantity, UnitOfMeasure}

sealed trait MetricKind extends Product
object MetricKind:
  enum Gauge extends MetricKind derives Encoder, Decoder:
    case Default, HealthCheck, Ratio

  enum Counter extends MetricKind derives Encoder, Decoder:
    case Default, Risk

  enum Meter extends MetricKind derives Encoder, Decoder:
    case Default

  enum Histogram extends MetricKind derives Encoder, Decoder:
    case Default

  enum Timer extends MetricKind derives Encoder, Decoder:
    case Default
end MetricKind

enum MetricCategory derives Encoder, Decoder:
  case Gauge(kind: MetricKind.Gauge, isCached: Boolean)
  case Counter(kind: MetricKind.Counter)
  case Meter(kind: MetricKind.Meter, squants: Squants)
  case Histogram(kind: MetricKind.Histogram, squants: Squants)
  case Timer(kind: MetricKind.Timer, squants: Squants = Squants(Each))
end MetricCategory

final case class Squants private (unitSymbol: String, dimensionName: String) derives Codec.AsObject
private object Squants:
  def apply[A <: Quantity[A]](um: UnitOfMeasure[A]): Squants =
    Squants(um.symbol, um(1).dimension.name)
end Squants

/** Per-instance identity for a metric, '''not''' merely its user-facing name.
  *
  * In both Dropwizard and OpenTelemetry a metric is process-scoped and keyed by its name: registering the
  * same name twice returns the same instrument (Dropwizard) or is a semantic conflict (OpenTelemetry). nanjin
  * deliberately does not work that way. Each instrument is acquired through a `Resource` whose lifetime is a
  * cats-effect scope, and every acquisition mints a fresh `MetricToken` even when the user-supplied `name`
  * string is identical. Two live instruments sharing a name are therefore distinct metrics, and nanjin
  * bypasses Dropwizard's name-based deduplication on purpose.
  *
  * Uniqueness comes from two stamped fields:
  *   - `age` — the monotonic clock reading (nanos) at construction, so instances have a stable creation order
  *     within a run.
  *   - `uniqueToken` — a hash of a fresh `cats.effect.Unique.Token`, so distinct acquisitions never collide
  *     even at the same instant.
  *
  * Both participate in `Eq` and in the encoded `MetricId.identifier` used as the Dropwizard registry key. The
  * constructor is private because a `MetricToken` can only be minted through the effectful `apply`; a caller
  * cannot fabricate one and thereby forge an identity or alias an existing instance.
  */
final case class MetricToken private (name: String, age: Long, uniqueToken: Int) derives Eq, Codec.AsObject
private object MetricToken:
  def apply[F[_]: {Applicative, Clock, Unique}](name: String): F[MetricToken] =
    (Clock[F].monotonic, Unique[F].unique).mapN((age, token) =>
      MetricToken(name, age.toNanos, Hash[Unique.Token].hash(token)))
end MetricToken

final case class MetricScope(label: String, domain: Domain, service: Service, task: Task)
    derives Codec.AsObject

/** The full identity of a single metric instance, and the bridge between nanjin's lifecycle model and its two
  * reporting backends.
  *
  * ===Lifecycle===
  * A nanjin metric is a '''scoped, uniquely-identified''' resource rather than a permanent, name-keyed
  * fixture of a global registry:
  *   - '''Scoped lifetime.''' Every instrument is a `Resource`; acquiring it registers under `identifier` and
  *     releasing it unregisters (e.g. `metricRegistry.remove(identifier)`). A metric is born and dies with
  *     its cats-effect scope, unlike a Dropwizard or OpenTelemetry instrument which lives for the life of the
  *     process.
  *   - '''Per-instance identity.''' Identity is carried by `token` (a `MetricToken`), which is unique per
  *     acquisition, not per name string. Same-named instruments across different scopes coexist without
  *     colliding or being conflated.
  *   - '''Windowed values (counters).''' A counter's Dropwizard value is reset on its reporting-window
  *     policy, so the stored value is cumulative only within the current window. The mirrored otel
  *     `UpDownCounter` is never reset; the backend owns windowing there.
  *
  * ===Two identities, keyed differently===
  * `MetricId` feeds both backends, but each keys on a different projection:
  *   - '''Dropwizard''' uses `identifier`, the whole `MetricId` encoded as compact JSON. Because that
  *     includes the per-instance `token` plus `category` (kind and unit), the key is '''finer''' than a name:
  *     identical names never collide, and risk vs. normal counters or same-name-different-unit siblings stay
  *     separate. `ScrapeMetrics` reconstructs the metric by decoding this string back into a `MetricId`,
  *     which is why `category` (and its unit) must live inside the identity.
  *   - '''OpenTelemetry''' uses only the raw name string plus the fixed point `attributes`
  *     (`nj.domain`/`nj.service`/`nj.task`). This is '''coarser''': `age`/`uniqueToken` and the risk flag are
  *     not projected, so same-named siblings collapse into a single otel series unless given distinct names.
  */
final case class MetricId(scope: MetricScope, token: MetricToken, category: MetricCategory)
    derives Codec.AsObject:
  val identifier: String = Encoder[MetricId].apply(this).noSpaces
  val attributes: List[Attribute[String]] =
    List(
      Attribute.from("nj.domain", scope.domain.value),
      Attribute.from("nj.service", scope.service.value),
      Attribute.from("nj.task", scope.task.value)
    )
end MetricId
