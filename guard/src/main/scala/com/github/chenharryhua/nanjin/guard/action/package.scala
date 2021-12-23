package com.github.chenharryhua.nanjin.guard

import com.github.chenharryhua.nanjin.guard.config.{Importance, MetricName}
import com.github.chenharryhua.nanjin.guard.event.EventPublisher.ATTENTION

package object action {
  // homebrew
  private[action] def alertMRName(name: MetricName, importance: Importance): String =
    importance match {
      case Importance.Critical => s"$ATTENTION.alert.[${name.value}]"
      case Importance.High     => s"10.warn.alert.[${name.value}]"
      case Importance.Medium   => s"80.info.alert.[${name.value}]"
      case Importance.Low      => s"80.debug.alert.[${name.value}]"
    }

  private[action] def passThroughMRName(name: MetricName, asError: Boolean): String =
    if (asError) s"$ATTENTION.pass.through.[${name.value}]" else s"11.pass.through.[${name.value}]"

  // delegate to dropwizard
  private[action] def counterMRName(name: MetricName, asError: Boolean): String =
    if (asError) s"$ATTENTION.counter.[${name.value}]" else s"20.counter.[${name.value}]"

  private[action] def meterMRName(name: MetricName, asError: Boolean): String =
    if (asError) s"$ATTENTION.meter.[${name.value}]" else s"21.meter.[${name.value}]"

  private[action] def histogramMRName(name: MetricName): String = s"22.histo.[${name.value}]"
}
