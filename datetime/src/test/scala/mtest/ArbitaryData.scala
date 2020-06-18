package mtest

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalTime, OffsetDateTime, ZoneId}

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8._
import com.github.chenharryhua.nanjin.datetime.{
  JavaLocalDate,
  JavaLocalDateTime,
  JavaLocalTime,
  JavaOffsetDateTime,
  JavaZonedDateTime,
  NJTimestamp
}
import org.scalacheck.{Arbitrary, Cogen}

object ArbitaryData {
  implicit val zoneId: ZoneId = ZoneId.systemDefault()

  // cogens

  implicit val coDate: Cogen[Date] =
    Cogen[Date]((a: Date) => a.getTime)

  implicit val coTimestamp: Cogen[Timestamp] =
    Cogen[Timestamp]((a: Timestamp) => a.getTime)

  implicit val coLocalDate: Cogen[LocalDate] =
    Cogen[LocalDate]((a: LocalDate) => a.toEpochDay)

  implicit val coJavaLocalDate: Cogen[JavaLocalDate] =
    Cogen[JavaLocalDate]((a: JavaLocalDate) => a.localDate.toEpochDay)

  implicit val coLocalTime: Cogen[LocalTime] =
    Cogen[LocalTime]((a: LocalTime) => a.toNanoOfDay)

  implicit val coJavaLocalTime: Cogen[JavaLocalTime] =
    Cogen[JavaLocalTime]((a: JavaLocalTime) => a.localTime.toNanoOfDay)

  implicit val coJavaLocalDateTime: Cogen[JavaLocalDateTime] =
    Cogen[JavaLocalDateTime]((a: JavaLocalDateTime) => a.localDateTime.toLocalTime.toNanoOfDay)

  implicit val coNJTimestamp: Cogen[NJTimestamp] =
    Cogen[NJTimestamp]((a: NJTimestamp) => a.milliseconds)

  implicit val coJavaZoned: Cogen[JavaZonedDateTime] =
    Cogen[JavaZonedDateTime]((a: JavaZonedDateTime) => a.zonedDateTime.toEpochSecond)

  implicit val coOffsetDateTime: Cogen[OffsetDateTime] =
    Cogen[OffsetDateTime]((a: OffsetDateTime) => a.toInstant.getEpochSecond)

  implicit val coJavaOffset: Cogen[JavaOffsetDateTime] =
    Cogen[JavaOffsetDateTime]((a: JavaOffsetDateTime) => a.offsetDateTime.toEpochSecond)

// arbs

  implicit val arbDate: Arbitrary[Date] = Arbitrary(
    genZonedDateTime.map(d => Date.valueOf(d.toLocalDate)))

  implicit val arbTimestamp: Arbitrary[Timestamp] = Arbitrary(
    genZonedDateTime.map(d => new Timestamp(d.toInstant.getEpochSecond)))

  implicit val arbKafkaTimestamp: Arbitrary[NJTimestamp] = Arbitrary(
    genZonedDateTime.map(d => NJTimestamp(d.toInstant.getEpochSecond)))

  implicit val arbJavaLocalDate: Arbitrary[JavaLocalDate] = Arbitrary(
    genZonedDateTime.map(zd => JavaLocalDate(zd.toLocalDate)))

  implicit val arbJavaLocalTime: Arbitrary[JavaLocalTime] = Arbitrary(
    genZonedDateTime.map(zd => JavaLocalTime(zd.toLocalDateTime.toLocalTime)))

  implicit val arbLocalTime: Arbitrary[LocalTime] = Arbitrary(
    genZonedDateTime.map(zd => zd.toLocalDateTime.toLocalTime))

  implicit val arbJavaLocalDateTime: Arbitrary[JavaLocalDateTime] = Arbitrary(
    genZonedDateTime.map(zd => JavaLocalDateTime(zd.toLocalDateTime)))

  implicit val arbJavaZoned: Arbitrary[JavaZonedDateTime] = Arbitrary(
    genZonedDateTime.map(zd => JavaZonedDateTime(zd))
  )

  implicit val arbJavaOffset: Arbitrary[JavaOffsetDateTime] = Arbitrary(
    genZonedDateTime.map(zd => JavaOffsetDateTime(zd.toOffsetDateTime))
  )

  implicit val arbJavaOffset2: Arbitrary[OffsetDateTime] = Arbitrary(
    genZonedDateTime.map(zd => OffsetDateTime.of(zd.toLocalDateTime, zd.getOffset))
  )
}
