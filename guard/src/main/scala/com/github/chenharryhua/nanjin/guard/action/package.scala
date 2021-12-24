package com.github.chenharryhua.nanjin.guard

import com.github.chenharryhua.nanjin.guard.config.{ActionParams, Importance, MetricName}

package object action {
  final val ATTENTION = "01.error"
  // homebrew
  private[action] def actionFailMRName(params: ActionParams): String = s"$ATTENTION.action.[${params.metricName.value}]"
  private[action] def actionSuccMRName(params: ActionParams): String = s"30.action.[${params.metricName.value}]"

  private[action] def alertMRName(name: MetricName, importance: Importance): String =
    importance match {
      case Importance.Critical => s"$ATTENTION.alert.[${name.value}]"
      case Importance.High     => s"10.warn.alert.[${name.value}]"
      case Importance.Medium   => s"80.info.alert.[${name.value}]"
      case Importance.Low      => s"80.debug.alert.[${name.value}]"
    }

  private[action] def passThroughMRName(name: MetricName, asError: Boolean): String =
    if (asError) s"$ATTENTION.passThrough.[${name.value}]" else s"11.passThrough.[${name.value}]"

  // delegate to dropwizard
  private[action] def counterMRName(name: MetricName, asError: Boolean): String =
    if (asError) s"$ATTENTION.counter.[${name.value}]" else s"20.counter.[${name.value}]"

  private[action] def meterMRName(name: MetricName, asError: Boolean): String =
    if (asError) s"$ATTENTION.meter.[${name.value}]" else s"21.meter.[${name.value}]"

  private[action] def histogramMRName(name: MetricName): String = s"22.histogram.[${name.value}]"
}
