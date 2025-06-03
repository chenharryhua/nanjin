package com.github.chenharryhua.nanjin.spark

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8.*
import com.github.chenharryhua.nanjin.spark.injection.*
import frameless.TypedEncoder
import org.scalacheck.Prop.{forAll, propBoolean}
import org.scalacheck.Properties

import java.sql.{Date, Timestamp}
import java.time.*

class TimeInjectionProps extends Properties("date time") {
  // must compile
  val date = TypedEncoder[Date]
  val timestamp = TypedEncoder[Timestamp]
  val localdate = TypedEncoder[LocalDate]
  val instant = TypedEncoder[Instant]

  property("spark timezone has no effect on epoch-second") = forAll { (ins: Instant) =>
    val tz1: ZoneId = ZoneId.of("Australia/Sydney")
    val tz2: ZoneId = ZoneId.of("America/Phoenix")
    ZonedDateTime.ofInstant(ins, tz1).toEpochSecond ==
      ZonedDateTime.ofInstant(ins, tz2).toEpochSecond
  }
}
