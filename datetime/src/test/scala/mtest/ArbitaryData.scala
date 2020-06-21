package mtest

import java.sql.{Date, Timestamp}
import java.time._

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8._
import com.github.chenharryhua.nanjin.datetime.{newyorkTime, NJTimestamp}
import org.scalacheck.{Arbitrary, Cogen, Gen}

object ArbitaryData {
  implicit val zoneId: ZoneId = newyorkTime

  // cogens

  implicit val coDate: Cogen[Date] =
    Cogen[Date]((a: Date) => a.getTime)

  implicit val coTimestamp: Cogen[Timestamp] =
    Cogen[Timestamp]((a: Timestamp) => a.getTime)

  implicit val coLocalDate: Cogen[LocalDate] =
    Cogen[LocalDate]((a: LocalDate) => a.toEpochDay)

  implicit val coLocalTime: Cogen[LocalTime] =
    Cogen[LocalTime]((a: LocalTime) => a.toNanoOfDay)

  implicit val coNJTimestamp: Cogen[NJTimestamp] =
    Cogen[NJTimestamp]((a: NJTimestamp) => a.milliseconds)

  implicit val coOffsetDateTime: Cogen[OffsetDateTime] =
    Cogen[OffsetDateTime]((a: OffsetDateTime) => a.toInstant.getEpochSecond)

// arbs

  val dateRange: Long = 700000

  implicit val arbDate: Arbitrary[Date] = Arbitrary(
    Gen.choose[Long](-dateRange, dateRange).map(d => Date.valueOf(LocalDate.ofEpochDay(d))))

  implicit val arbLocalDate: Arbitrary[LocalDate] =
    Arbitrary(Gen.choose[Long](-dateRange, dateRange).map(d => LocalDate.ofEpochDay(d.toLong)))

  implicit val arbTimestamp: Arbitrary[Timestamp] = Arbitrary(
    genZonedDateTime.map(d => new Timestamp(d.toInstant.getEpochSecond)))

  implicit val arbKafkaTimestamp: Arbitrary[NJTimestamp] = Arbitrary(
    genZonedDateTime.map(d => NJTimestamp(d.toInstant.getEpochSecond)))

  implicit val arbLocalDateTime: Arbitrary[LocalDateTime] = Arbitrary(
    genZonedDateTimeWithZone(Some(zoneId)).map(zd => zd.toLocalDateTime))

  implicit val arbOffsetDateTime: Arbitrary[OffsetDateTime] = Arbitrary(
    genZonedDateTimeWithZone(Some(zoneId)).map(zd =>
      OffsetDateTime.of(zd.toLocalDateTime, zd.getOffset))
  )

  implicit val arbZonedDateTime: Arbitrary[ZonedDateTime] = Arbitrary(
    genZonedDateTimeWithZone(Some(zoneId))
  )
}
