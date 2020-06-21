package mtest

import java.sql.{Date, Timestamp}
import java.time._

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8.arbInstantJdk8
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.datetime.transformers._
import io.scalaland.chimney.dsl._
import org.scalacheck.Prop.{forAll, propBoolean}
import org.scalacheck.Properties

class TransformersTest extends Properties("date time") {
  import ArbitaryData._
  property("transformer - zoned-date-time") = forAll { (zd: ZonedDateTime) =>
    zd.transformInto[Timestamp].transformInto[ZonedDateTime] == zd
    zd.transformInto[Instant].transformInto[ZonedDateTime] == zd
  }

  property("transformer - local-date-time") = forAll { (zd: LocalDateTime) =>
    zd.transformInto[Timestamp].transformInto[LocalDateTime] == zd
    zd.transformInto[Instant].transformInto[LocalDateTime] == zd
  }

  property("transformer - offset-date-time") = forAll { (zd: OffsetDateTime) =>
    zd.transformInto[Timestamp].transformInto[OffsetDateTime] == zd
    zd.transformInto[Instant].transformInto[OffsetDateTime] == zd
  }

  property("transformer - instant") = forAll { (zd: Instant) =>
    zd.transformInto[Timestamp].transformInto[Instant] == zd
    zd.transformInto[Instant].transformInto[Instant] == zd
  }

  property("transformer - timestamp") = forAll { (zd: Timestamp) =>
    zd.transformInto[Timestamp].transformInto[Timestamp] == zd
    zd.transformInto[Instant].transformInto[Timestamp] == zd
  }

  property("transformer - local-date") = forAll { (zd: LocalDate) =>
    zd.transformInto[Date].transformInto[LocalDate] == zd
    zd.transformInto[Date].transformInto[LocalDate] == zd
  }

  property("transformer - sql-date") = forAll { (zd: Date) =>
    zd.transformInto[LocalDate].transformInto[Date] == zd
    zd.transformInto[LocalDate].transformInto[Date] == zd
  }

}
