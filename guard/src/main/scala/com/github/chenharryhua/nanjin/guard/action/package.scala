package com.github.chenharryhua.nanjin.guard

import com.github.chenharryhua.nanjin.guard.config.{ActionParams, Importance, MetricName}

package object action {

  /** 01 - 09: Errors
    *
    * 10 - 19: Warnings
    *
    * 20 - 29: Info
    */

  // homebrew
  private[guard] val servicePanicMRName: String   = "01.service.panic"
  private[guard] val serviceRestartMRName: String = "12.service.start"

  private[action] def actionFailMRName(params: ActionParams): String  = s"07.action.fail.[${params.metricName.value}]"
  private[action] def actionRetryMRName(params: ActionParams): String = s"11.action.retry.[${params.metricName.value}]"
  private[action] def actionSuccMRName(params: ActionParams): String  = s"27.action.succ.[${params.metricName.value}]"
  private[action] def actionTimerMRName(params: ActionParams): String = s"action.[${params.metricName.value}]"

  private[action] def alertMRName(name: MetricName, importance: Importance): String =
    importance match {
      case Importance.High   => s"02.alert.error.[${name.value}]"
      case Importance.Medium => s"10.alert.warn.[${name.value}]"
      case Importance.Low    => s"21.alert.info.[${name.value}]"
    }

  private[action] def passThroughMRName(name: MetricName, asError: Boolean, countOrMeter: Boolean): String =
    (asError, countOrMeter) match {
      case (true, true)   => s"03.passThroughC.error.[${name.value}]"
      case (true, false)  => s"04.passThroughM.error.[${name.value}]"
      case (false, true)  => s"22.passThroughC.[${name.value}]"
      case (false, false) => s"23.passThroughM.[${name.value}]"
    }

  // delegate to dropwizard
  private[action] def counterMRName(name: MetricName, asError: Boolean): String =
    if (asError) s"05.counter.error.[${name.value}]" else s"24.counter.[${name.value}]"

  private[action] def meterMRName(name: MetricName, asError: Boolean): String =
    if (asError) s"06.meter.error.[${name.value}]" else s"25.meter.[${name.value}]"

  private[action] def histogramMRName(name: MetricName): String = s"26.histogram.[${name.value}]"
}
