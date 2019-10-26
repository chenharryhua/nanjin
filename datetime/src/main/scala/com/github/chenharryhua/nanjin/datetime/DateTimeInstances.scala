package com.github.chenharryhua.nanjin.datetime

import java.sql.{Date, Timestamp}

import cats.implicits._
import cats.{Hash, Order, Show}
import io.chrisdavenport.cats.time.instances.all

private[datetime] trait DateTimeInstances extends all {

  implicit final val timestampInstance: Hash[Timestamp] with Order[Timestamp] with Show[Timestamp] =
    new Hash[Timestamp] with Order[Timestamp] with Show[Timestamp] {
      override def hash(x: Timestamp): Int                  = x.hashCode
      override def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
      override def show(x: Timestamp): String               = x.toString
    }

  implicit final val NJTimestampInstance
    : Hash[NJTimestamp] with Order[NJTimestamp] with Show[NJTimestamp] =
    new Hash[NJTimestamp] with Order[NJTimestamp] with Show[NJTimestamp] {
      override def hash(x: NJTimestamp): Int                    = x.hashCode
      override def compare(x: NJTimestamp, y: NJTimestamp): Int = x.utc.compareTo(y.utc)
      override def show(x: NJTimestamp): String                 = x.utc.toString
    }

  implicit final val JavaZonedInstance
    : Hash[JavaZonedDateTime] with Order[JavaZonedDateTime] with Show[JavaZonedDateTime] =
    new Hash[JavaZonedDateTime] with Order[JavaZonedDateTime] with Show[JavaZonedDateTime] {
      override def hash(x: JavaZonedDateTime): Int = x.hashCode

      override def compare(x: JavaZonedDateTime, y: JavaZonedDateTime): Int =
        x.zonedDateTime.compareTo(y.zonedDateTime)
      override def show(x: JavaZonedDateTime): String = x.zonedDateTime.show
    }

  implicit final val JavaOffsetInstance
    : Hash[JavaOffsetDateTime] with Order[JavaOffsetDateTime] with Show[JavaOffsetDateTime] =
    new Hash[JavaOffsetDateTime] with Order[JavaOffsetDateTime] with Show[JavaOffsetDateTime] {
      override def hash(x: JavaOffsetDateTime): Int = x.hashCode

      override def compare(x: JavaOffsetDateTime, y: JavaOffsetDateTime): Int =
        x.offsetDateTime.compareTo(y.offsetDateTime)
      override def show(x: JavaOffsetDateTime): String = x.offsetDateTime.show
    }

  implicit final val DateInstance: Hash[Date] with Order[Date] with Show[Date] =
    new Hash[Date] with Order[Date] with Show[Date] {
      override def hash(x: Date): Int             = x.hashCode
      override def compare(x: Date, y: Date): Int = x.compareTo(y)
      override def show(x: Date): String          = x.toString
    }
}
