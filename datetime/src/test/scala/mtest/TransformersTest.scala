package mtest

import com.github.chenharryhua.nanjin.common.transformers.*
import com.github.chenharryhua.nanjin.datetime.instances.*
import io.scalaland.chimney.dsl.*
import org.scalacheck.Prop.{forAll, propBoolean}
import org.scalacheck.Properties

import java.sql.{Date, Timestamp}
import java.time.*

class TransformersTest extends Properties("isomorphic chimney transformers") {
  import ArbitaryData.*

  property("instant") = forAll { (zd: Instant) =>
    zd.transformInto[Timestamp].transformInto[Instant] == zd &&
    zd.transformInto[Instant].transformInto[Instant] == zd
  }

  property("timestamp") = forAll { (zd: Timestamp) =>
    zd.transformInto[Timestamp].transformInto[Timestamp] == zd &&
    zd.transformInto[Instant].transformInto[Timestamp] == zd
  }

  property("local-date") = forAll { (zd: LocalDate) =>
    zd.transformInto[LocalDate].transformInto[LocalDate] == zd &&
    zd.transformInto[Date].transformInto[LocalDate] == zd
  }

  property("sql-date") = forAll { (zd: Date) =>
    zd.transformInto[LocalDate].transformInto[Date] == zd &&
    zd.transformInto[Date].transformInto[Date] == zd
  }
}
