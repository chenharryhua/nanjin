package mtest

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8.{arbInstantJdk8, arbLocalDateTimeJdk8}
import com.github.chenharryhua.nanjin.common.transformers.*
import com.github.chenharryhua.nanjin.datetime.*
import com.github.chenharryhua.nanjin.datetime.instances.*
import io.scalaland.chimney.dsl.*
import org.scalacheck.Prop.{forAll, propBoolean}
import org.scalacheck.Properties

import java.sql.{Date, Timestamp}
import java.time.*

class TransformersTest extends Properties("isomorphic chimney transformers") {
  import ArbitaryData.*
  property("zoned-date-time") = forAll { (zd: ZonedDateTime) =>
    (zd.transformInto[Timestamp].transformInto[ZonedDateTime] == zd) &&
    (zd.transformInto[Instant].transformInto[ZonedDateTime] == zd)
  }

  property("local-date-time") = forAll { (zd: LocalDateTime) =>
    (zd.transformInto[Timestamp].transformInto[LocalDateTime] == zd) &&
    (zd.transformInto[Instant].transformInto[LocalDateTime] == zd)
  }

  property("offset-date-time") = forAll { (zd: OffsetDateTime) =>
    (zd.transformInto[Timestamp].transformInto[OffsetDateTime] == zd) &&
    (zd.transformInto[Instant].transformInto[OffsetDateTime] == zd)
  }

  property("instant") = forAll { (zd: Instant) =>
    (zd.transformInto[Timestamp].transformInto[Instant] == zd) &&
    (zd.transformInto[Instant].transformInto[Instant] == zd)
  }

  property("timestamp") = forAll { (zd: Timestamp) =>
    (zd.transformInto[Timestamp].transformInto[Timestamp] == zd) &&
    (zd.transformInto[Instant].transformInto[Timestamp] == zd)
  }

  property("local-date") = forAll { (zd: LocalDate) =>
    (zd.transformInto[LocalDate].transformInto[LocalDate] == zd) &&
    (zd.transformInto[Date].transformInto[LocalDate] == zd)
  }

  property("sql-date") = forAll { (zd: Date) =>
    (zd.transformInto[LocalDate].transformInto[Date] == zd) &&
    (zd.transformInto[Date].transformInto[Date] == zd)
  }
}
