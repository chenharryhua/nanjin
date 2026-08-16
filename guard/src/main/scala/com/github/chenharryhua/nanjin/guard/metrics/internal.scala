package com.github.chenharryhua.nanjin.guard.metrics

import io.circe.{Decoder, Encoder}

sealed private trait CategoryKind extends Product
private object CategoryKind:
  enum GaugeKind extends CategoryKind derives Encoder, Decoder:
    case Gauge, HealthCheck, Ratio

  enum CounterKind extends CategoryKind derives Encoder, Decoder:
    case Counter, Risk

  enum MeterKind extends CategoryKind derives Encoder, Decoder:
    case Meter

  enum HistogramKind extends CategoryKind derives Encoder, Decoder:
    case Histogram

  enum TimerKind extends CategoryKind derives Encoder, Decoder:
    case Timer
end CategoryKind

private enum Category derives Encoder, Decoder:
  case Gauge(kind: CategoryKind.GaugeKind)
  case Counter(kind: CategoryKind.CounterKind)
  case Meter(kind: CategoryKind.MeterKind, squants: Squants)
  case Histogram(kind: CategoryKind.HistogramKind, squants: Squants)
  case Timer(kind: CategoryKind.TimerKind)
end Category
