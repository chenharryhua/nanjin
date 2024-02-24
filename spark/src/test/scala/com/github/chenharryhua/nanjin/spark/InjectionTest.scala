package com.github.chenharryhua.nanjin.spark

import cats.kernel.laws.discipline.OrderTests
import cats.tests.CatsSuite
import frameless.{SQLDate, SQLTimestamp}
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class InjectionTest extends CatsSuite with FunSuiteDiscipline with InjectionInstances {

  implicit val coDate: Cogen[SQLDate] =
    Cogen[SQLDate]((a: SQLDate) => a.days.toLong)

  implicit val coTimestamp: Cogen[SQLTimestamp] =
    Cogen[SQLTimestamp]((a: SQLTimestamp) => a.us)

  implicit val arbDate: Arbitrary[SQLDate]           = Arbitrary(Gen.posNum[Int].map(SQLDate(_)))
  implicit val arbTimestamp: Arbitrary[SQLTimestamp] = Arbitrary(Gen.posNum[Long].map(SQLTimestamp(_)))

  checkAll("SQLDate", OrderTests[SQLDate].order)
  checkAll("SQLTimestamp", OrderTests[SQLTimestamp].order)
}
