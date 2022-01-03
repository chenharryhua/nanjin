package com.github.chenharryhua.nanjin.guard

import com.github.chenharryhua.nanjin.guard.config.{ActionParams, DigestedName, Importance}

package object action {

  /** 01 - 09: Errors
    *
    * 10 - 19: Warnings
    *
    * 20 - 29: Info
    *
    * > 30 reserved
    *
    * only counters are periodically reset
    */

  private[guard] val servicePanicMRName: String   = "01.service.panic"
  private[guard] val serviceRestartMRName: String = "12.service.start"

  private[action] def alertMRName(name: DigestedName, importance: Importance): String =
    importance match {
      case Importance.Critical => s"02.alert.${name.metricRepr}.error"
      case Importance.High     => s"10.alert.${name.metricRepr}.warn"
      case Importance.Medium   => s"20.alert.${name.metricRepr}.info"
      case Importance.Low      => s"21.alert.${name.metricRepr}.debug"
    }

  private[action] def passThroughMRName(name: DigestedName, asError: Boolean): String =
    if (asError) s"03.passThrough.${name.metricRepr}.error" else s"22.passThrough.${name.metricRepr}"

  private[action] def counterMRName(name: DigestedName, asError: Boolean): String =
    if (asError) s"04.counter.${name.metricRepr}.error" else s"23.counter.${name.metricRepr}"

  private[action] def meterMRName(name: DigestedName): String     = s"24.meter.${name.metricRepr}"
  private[action] def histogramMRName(name: DigestedName): String = s"25.histogram.${name.metricRepr}"

  private[action] def actionFailMRName(ap: ActionParams): String  = s"05.${ap.alias}.${ap.metricName.metricRepr}.fail"
  private[action] def actionRetryMRName(ap: ActionParams): String = s"11.${ap.alias}.${ap.metricName.metricRepr}.retry"
  private[action] def actionSuccMRName(ap: ActionParams): String  = s"26.${ap.alias}.${ap.metricName.metricRepr}.succ"
  private[action] def actionTimerMRName(ap: ActionParams): String = s"${ap.alias}.${ap.metricName.metricRepr}"
}
