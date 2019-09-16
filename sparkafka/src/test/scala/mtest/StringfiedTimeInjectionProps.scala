package mtest

import java.time.{Instant, LocalDateTime, ZonedDateTime}

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8._
import com.github.chenharryhua.nanjin.sparkafka.StringfiedTimeInjection._
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
}
