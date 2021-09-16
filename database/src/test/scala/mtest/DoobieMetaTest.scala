package mtest

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

import java.time.*

class DoobieMetaTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {
  implicit val zoneId: ZoneId = ZoneId.systemDefault()
}
