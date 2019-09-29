package mtest

import java.time.{Instant, LocalDate, LocalDateTime}

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8._
import com.github.chenharryhua.nanjin.sparkdb._
import org.scalacheck.Prop.{forAll, propBoolean}
import org.scalacheck.Properties

class TimeInjectionProps extends Properties("Injection") {

  property("Instant identity") = forAll { (dt: Instant) =>
    instantInjection.invert(instantInjection(dt)) == dt
  }

  property("LocalDateTime identity") = forAll { (dt: LocalDateTime) =>
    localDateTimeInjection.invert(localDateTimeInjection(dt)) == dt
  }

  property("ZonedDateTime identity") = forAll { (dt: Instant) =>
    zonedDateTimeInjection(zonedDateTimeInjection.invert((dt))) == dt
  }

  property("localDate identity") = forAll { (dt: LocalDate) =>
    (dt.getYear > -1000000L) ==> (localDateInjection.invert(localDateInjection(dt)) == dt)
  }
}
