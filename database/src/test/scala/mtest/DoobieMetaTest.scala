package mtest

import java.sql.{Date, Timestamp}
import java.time._

import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class DoobieMetaTest extends AnyFunSuite with FunSuiteDiscipline {
  implicit val zoneId: ZoneId    = ZoneId.systemDefault()
}
