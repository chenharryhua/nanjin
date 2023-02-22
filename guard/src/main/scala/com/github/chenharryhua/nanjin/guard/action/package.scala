package com.github.chenharryhua.nanjin.guard

import com.github.chenharryhua.nanjin.guard.config.{ActionParams, AlertLevel, Digested}
package object action {

  private[action] def alertMRName(name: Digested, level: AlertLevel): String =
    level match {
      case AlertLevel.Error => s"alert.${name.metricRepr}.error"
      case AlertLevel.Warn  => s"alert.${name.metricRepr}.warn"
      case AlertLevel.Info  => s"alert.${name.metricRepr}.info"
    }

  private[action] def passThroughMRName(name: Digested, asError: Boolean): String =
    if (asError) s"passThrough.${name.metricRepr}.error" else s"passThrough.${name.metricRepr}"

  private[action] def counterMRName(name: Digested): String = s"counter.${name.metricRepr}"

  private[action] def meterMRName(name: Digested): String     = s"meter.${name.metricRepr}"
  private[action] def histogramMRName(name: Digested): String = s"histogram.${name.metricRepr}"
  private[action] def gaugeMRName(name: Digested): String     = s"gauge.${name.metricRepr}"

  private[action] def actionFailMRName(ap: ActionParams): String  = s"action.${ap.digested.metricRepr}.fail"
  private[action] def actionSuccMRName(ap: ActionParams): String  = s"action.${ap.digested.metricRepr}.succ"
  private[action] def actionTimerMRName(ap: ActionParams): String = s"action.${ap.digested.metricRepr}"

}
