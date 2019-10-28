package mtest

import java.sql.{Date, Timestamp}
import java.time.{OffsetDateTime, ZoneId}

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8._
import com.github.chenharryhua.nanjin.datetime.{JavaOffsetDateTime, JavaZonedDateTime, NJTimestamp}
import org.scalacheck.{Arbitrary, Cogen, Gen}

object ArbitaryData {
  implicit val zoneId: ZoneId = ZoneId.systemDefault()

  implicit val coTimestamp: Cogen[Timestamp] =
    Cogen[Timestamp]((a: Timestamp) => a.getTime)

  implicit val coDate: Cogen[Date] =
    Cogen[Date]((a: Date) => a.getTime)

  implicit val coNJTimestamp: Cogen[NJTimestamp] =
    Cogen[NJTimestamp]((a: NJTimestamp) => a.milliseconds)

  implicit val coJavaZoned: Cogen[JavaZonedDateTime] =
    Cogen[JavaZonedDateTime]((a: JavaZonedDateTime) => a.zonedDateTime.toEpochSecond)

  implicit val coOffsetDateTime: Cogen[OffsetDateTime] =
    Cogen[OffsetDateTime]((a: OffsetDateTime) => a.toInstant.getEpochSecond)

  implicit val coJavaOffset: Cogen[JavaOffsetDateTime] =
    Cogen[JavaOffsetDateTime]((a: JavaOffsetDateTime) => a.offsetDateTime.toEpochSecond)

  implicit val arbTimestamp: Arbitrary[Timestamp] = Arbitrary(
    genZonedDateTimeWithZone(None).map(d => new Timestamp(d.toInstant.getEpochSecond)))

  implicit val arbKafkaTimestamp: Arbitrary[NJTimestamp] = Arbitrary(
    genZonedDateTimeWithZone(None).map(d => NJTimestamp(d.toInstant.getEpochSecond)))

  implicit val arbDate: Arbitrary[Date] = Arbitrary(
    genZonedDateTimeWithZone(None).map(d => Date.valueOf(d.toLocalDate)))

  implicit val arbJavaZoned: Arbitrary[JavaZonedDateTime] = Arbitrary(
    genZonedDateTimeWithZone(None).map(zd => JavaZonedDateTime(zd))
  )

  implicit val arbJavaOffset: Arbitrary[JavaOffsetDateTime] = Arbitrary(
    genZonedDateTimeWithZone(None).map(zd => JavaOffsetDateTime(zd.toOffsetDateTime))
  )

  implicit val arbJavaOffset2: Arbitrary[OffsetDateTime] = Arbitrary(
    genZonedDateTimeWithZone(None).map(zd => OffsetDateTime.of(zd.toLocalDateTime, zd.getOffset))
  )
}
