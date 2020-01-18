package mtest

import java.sql.{Date, Timestamp}
import java.time._

import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.FunSuiteDiscipline
import org.scalatest.prop.Configuration

class DoobieMetaTest extends AnyFunSuite with FunSuiteDiscipline with Configuration{
  implicit val zoneId: ZoneId    = ZoneId.systemDefault()
}
