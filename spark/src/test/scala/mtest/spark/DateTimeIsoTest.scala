package mtest.spark

import java.sql.{Date, Timestamp}
import java.time._

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8.arbInstantJdk8
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.{SQLDate, SQLTimestamp}
import monocle.law.discipline.IsoTests
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class DateTimeIsoTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {
  import ArbitaryData._

  checkAll("spark sql-timestamp", IsoTests[Timestamp, SQLTimestamp](isoSQLTimestamp))

  checkAll("spark sql-date", IsoTests[Date, SQLDate](isoSQLDate))

}
