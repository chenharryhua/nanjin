package mtest

import java.sql.Date
import java.time.LocalDate

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8.genZonedDateTimeWithZone
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.datetime.iso._
import monocle.Iso
import monocle.law.discipline.IsoTests
import org.scalacheck._
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class DateIsoTest extends AnyFunSuite with FunSuiteDiscipline {

  val genAfter1900 =
    genZonedDateTimeWithZone(None).map(_.toLocalDate).filter(_.isAfter(LocalDate.of(1900, 1, 1)))

  implicit val arbDate = Arbitrary(
    genAfter1900.map(d => Date.valueOf(d))
  )

  implicit val arbLocalDate = Arbitrary(genAfter1900)

  implicit val coDate = Cogen[Date]((d: Date) => d.getTime())

  checkAll("local-date", IsoTests[LocalDate, Date](implicitly[Iso[LocalDate, Date]]))

}
