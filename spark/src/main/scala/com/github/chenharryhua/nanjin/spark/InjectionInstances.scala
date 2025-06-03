package com.github.chenharryhua.nanjin.spark

import frameless.Injection
import shapeless.Witness

import java.sql.Date
import java.time.LocalDate

private[spark] trait InjectionInstances extends Serializable {

  implicit val localDateInjection: Injection[LocalDate, Date] =
    new Injection[LocalDate, Date] {
      override def apply(a: LocalDate): Date = Date.valueOf(a)
      override def invert(b: Date): LocalDate = b.toLocalDate
    }

  // enums
  implicit def enumToStringInjection[E <: Enumeration](implicit
    w: Witness.Aux[E]): Injection[E#Value, String] =
    Injection(_.toString, w.value.withName(_))
}
