package mtest.spark.persist

import java.sql.Timestamp
import java.time.Instant

import frameless.TypedDataset
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

object RoosterData {
  val instant: Instant     = Instant.parse("2012-10-26T18:00:00Z")
  val timestamp: Timestamp = Timestamp.from(instant)

  val nowInstant: Instant     = Instant.now
  val nowTimestamp: Timestamp = Timestamp.from(nowInstant)

  val data: List[Rooster] =
    List(
      Rooster(1, instant, timestamp, BigDecimal("1234.567"), BigDecimal("654321.0")),
      Rooster(2, nowInstant, nowTimestamp, BigDecimal("1234.5678"), BigDecimal("654321.1")),
      Rooster(3, instant, nowTimestamp, BigDecimal("1234.56789"), BigDecimal("654321.5")),
      Rooster(4, nowInstant, timestamp, BigDecimal("0.123456"), BigDecimal("0.654321"))
    )

  val expected: Set[Rooster] =
    Set(
      Rooster(1, instant, timestamp, BigDecimal("1234.567"), BigDecimal("654321")),
      Rooster(2, nowInstant, nowTimestamp, BigDecimal("1234.568"), BigDecimal("654321")),
      Rooster(3, instant, nowTimestamp, BigDecimal("1234.568"), BigDecimal("654322")),
      Rooster(4, nowInstant, timestamp, BigDecimal("0.123"), BigDecimal("1"))
    )

  val rdd: RDD[Rooster] = sparkSession.sparkContext.parallelize(data)

  val ds: Dataset[Rooster] = TypedDataset.create(rdd).dataset

}
