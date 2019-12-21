package mtest

import java.sql.{Date, Timestamp}
import java.time._

import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class DoobieMetaTest extends AnyFunSuite with Discipline {
  implicit val zoneId: ZoneId    = ZoneId.systemDefault()
}
