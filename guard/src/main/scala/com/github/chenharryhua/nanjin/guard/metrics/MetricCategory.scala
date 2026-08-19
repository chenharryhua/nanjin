package com.github.chenharryhua.nanjin.guard.metrics

import io.circe.{Decoder, Encoder}

sealed trait MetricKind extends Product
object MetricKind:
  enum Gauge extends MetricKind derives Encoder, Decoder:
    case Default, HealthCheck, Percentile

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
  case Gauge(kind: MetricKind.Gauge)
  case Counter(kind: MetricKind.Counter)
  case Meter(kind: MetricKind.Meter, squants: Squants)
  case Histogram(kind: MetricKind.Histogram, squants: Squants)
  case Timer(kind: MetricKind.Timer)
end MetricCategory
