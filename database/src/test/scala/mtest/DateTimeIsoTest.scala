package mtest

import java.sql.{Date, Timestamp}
import java.time._
import java.util.GregorianCalendar

import cats.Eq
import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8.{arbLocalDateJdk8 => _, _}
import com.github.chenharryhua.nanjin.database._
import doobie.util.Meta
import monocle.law.discipline.IsoTests
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline
import monocle.Iso

class DateTimeIsoTest extends AnyFunSuite with Discipline {
  implicit val zoneId: ZoneId = ZoneId.systemDefault()

}
