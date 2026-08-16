package com.github.chenharryhua.nanjin.guard.metrics

import io.circe.{Decoder, Encoder}

sealed trait MetricCategoryKind extends Product
object MetricCategoryKind:
  enum GaugeKind extends MetricCategoryKind derives Encoder, Decoder:
    case Default, HealthCheck, Ratio

  enum CounterKind extends MetricCategoryKind derives Encoder, Decoder:
    case Default, Risk

  enum MeterKind extends MetricCategoryKind derives Encoder, Decoder:
    case Default

  enum HistogramKind extends MetricCategoryKind derives Encoder, Decoder:
    case Default

  enum TimerKind extends MetricCategoryKind derives Encoder, Decoder:
    case Default
end MetricCategoryKind

enum MetricCategory derives Encoder, Decoder:
  case GaugeC(kind: MetricCategoryKind.GaugeKind)
  case CounterC(kind: MetricCategoryKind.CounterKind)
  case MeterC(kind: MetricCategoryKind.MeterKind, squants: Squants)
  case HistogramC(kind: MetricCategoryKind.HistogramKind, squants: Squants)
  case TimerC(kind: MetricCategoryKind.TimerKind)
end MetricCategory
