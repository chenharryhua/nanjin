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
    */

  // homebrew
  private[guard] val servicePanicMRName: String   = "01.service.panic"
  private[guard] val serviceRestartMRName: String = "12.service.start"

  private[action] def actionFailMRName(ap: ActionParams): String  = s"07.${ap.alias}.${ap.name.value}.fail"
  private[action] def actionRetryMRName(ap: ActionParams): String = s"11.${ap.alias}.${ap.name.value}.retry"
  private[action] def actionSuccMRName(ap: ActionParams): String  = s"27.${ap.alias}.${ap.name.value}.succ"
  private[action] def actionTimerMRName(ap: ActionParams): String = s"${ap.alias}.${ap.name.value}"

  private[action] def alertMRName(name: DigestedName, importance: Importance): String =
    importance match {
      case Importance.Critical => s"02.alert.[${name.value}].error"
      case Importance.High     => s"10.alert.[${name.value}].warn"
      case Importance.Medium   => s"20.alert.[${name.value}].info"
      case Importance.Low      => s"21.alert.[${name.value}].debug"
    }

  private[action] def passThroughMRName(name: DigestedName, asError: Boolean, counterOrMeter: Boolean): String =
    (asError, counterOrMeter) match {
      case (true, true)   => s"03.passThroughC.[${name.value}].error"
      case (true, false)  => s"04.passThroughM.[${name.value}].error"
      case (false, true)  => s"22.passThroughC.[${name.value}]"
      case (false, false) => s"23.passThroughM.[${name.value}]"
    }

  // delegate to dropwizard
  private[action] def counterMRName(name: DigestedName, asError: Boolean): String =
    if (asError) s"05.counter.[${name.value}].error" else s"24.counter.[${name.value}]"

  private[action] def meterMRName(name: DigestedName, asError: Boolean): String =
    if (asError) s"06.meter.[${name.value}].error" else s"25.meter.[${name.value}]"

  private[action] def histogramMRName(name: DigestedName): String = s"26.histogram.[${name.value}]"
}
