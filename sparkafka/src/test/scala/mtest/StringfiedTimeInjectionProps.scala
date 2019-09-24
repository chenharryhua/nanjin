package mtest

import java.sql.Date
import java.time.{Instant, LocalDate, LocalDateTime, ZonedDateTime}

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8._
import com.github.chenharryhua.nanjin.sparkafka.DatetimeInstances._
import org.scalacheck.Prop.forAll
import org.scalacheck.Properties

class StringfiedTimeInjectionProps extends Properties("Injection") {

  property("Instant identity") = forAll { (dt: Instant) =>
    instantInjection.invert(instantInjection(dt)) == dt
  }

  property("LocalDateTime identity") = forAll { (dt: LocalDateTime) =>
    localDateTimeInjection.invert(localDateTimeInjection(dt)) == dt
  }

  property("ZonedDateTime identity") = forAll { (dt: ZonedDateTime) =>
    zonedDateTimeInjection.invert(zonedDateTimeInjection(dt)) == dt
  }

  property("localDate identity") = forAll { (dt: LocalDate) =>
    localDateInjection.invert(localDateInjection(dt)) == dt
  }

  property("sql Date identity") = forAll { (dt: LocalDate) =>
    sqlDateInjection.invert(sqlDateInjection(Date.valueOf(dt))) == Date.valueOf(dt)
  }

  property("sql Timestamp identity") = forAll { (dt: Instant) =>
    sqlTimestampInjection(sqlTimestampInjection.invert(dt.getEpochSecond)) == dt.getEpochSecond
  }
}
