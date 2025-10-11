package mtest.spark.persist

import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import mtest.spark.*
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit

object RoosterData {
  val instant: Instant = Instant.parse("2012-10-26T18:00:00Z")
  val timestamp: Timestamp = Timestamp.from(instant)

  val nowInstant: Instant = Instant.now.truncatedTo(ChronoUnit.MILLIS)
  val nowTimestamp: Timestamp = Timestamp.from(nowInstant)

  val data: List[Rooster] =
    List(
      Rooster(1, instant, timestamp, BigDecimal("1234.567"), BigDecimal("654321.0"), None),
      Rooster(2, nowInstant, nowTimestamp, BigDecimal("1234.5678"), BigDecimal("654321.1"), None),
      Rooster(3, instant, nowTimestamp, BigDecimal("1234.56789"), BigDecimal("654321.5"), Some(1)),
      Rooster(4, nowInstant, timestamp, BigDecimal("0.123456"), BigDecimal("0.654321"), Some(2))
    )

  val expected: Set[Rooster] =
    Set(
      Rooster(1, instant, timestamp, BigDecimal("1234.567"), BigDecimal("654321"), None),
      Rooster(2, nowInstant, nowTimestamp, BigDecimal("1234.568"), BigDecimal("654321"), None),
      Rooster(3, instant, nowTimestamp, BigDecimal("1234.568"), BigDecimal("654322"), Some(1)),
      Rooster(4, nowInstant, timestamp, BigDecimal("0.123"), BigDecimal("1"), Some(2))
    )

  val rdd: RDD[Rooster] = sparkSession.sparkContext.parallelize(data)

  import sparkSession.implicits.*
  val ds: Dataset[Rooster] = sparkSession.loadData(data)

  val bigset: Dataset[Rooster] = sparkSession.createDataset(
    sparkSession.sparkContext.parallelize(
      List.fill(1000)(Rooster(0, instant, timestamp, BigDecimal("0"), BigDecimal("0"), None))))

}
