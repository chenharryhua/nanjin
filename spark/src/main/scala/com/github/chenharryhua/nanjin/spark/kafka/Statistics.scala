package com.github.chenharryhua.nanjin.spark.kafka

import cats.Endo
import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.{dayResolution, hourResolution, minuteResolution}
import com.github.chenharryhua.nanjin.messages.kafka.{CRMetaInfo, ZonedCRMetaInfo}
import com.github.chenharryhua.nanjin.spark.utils
import org.apache.spark.sql.Dataset

import java.time.ZoneId

final class Statistics private[spark] (val dataset: Dataset[CRMetaInfo]) extends Serializable {
  private val zoneId: ZoneId = utils.sparkZoneId(dataset.sparkSession)
  import dataset.sparkSession.implicits.*

  def cherryPick[F[_]](partition: Int, offset: Long)(implicit F: Sync[F]): F[List[ZonedCRMetaInfo]] =
    F.interruptible {
      dataset
        .filter(m => m.offset === offset && m.partition === partition)
        .collect()
        .toList
        .map(_.zoned(zoneId))
    }

  def transform(f: Endo[Dataset[CRMetaInfo]]): Statistics =
    new Statistics(f(dataset))

  def union(other: Statistics): Statistics =
    transform(_.union(other.dataset))

  def minutely[F[_]](implicit F: Sync[F]): F[List[MinutelyResult]] =
    F.interruptible {
      dataset
        .map(m => m.localDateTime(zoneId).getMinute)
        .groupByKey(identity)
        .mapGroups((m, iter) => MinutelyResult(m, iter.size))
        .orderBy("minute")
        .collect()
        .toList
    }

  def hourly[F[_]](implicit F: Sync[F]): F[List[HourlyResult]] =
    F.interruptible {
      dataset
        .map(m => m.localDateTime(zoneId).getHour)
        .groupByKey(identity)
        .mapGroups((m, iter) => HourlyResult(m, iter.size))
        .orderBy("hour")
        .collect()
        .toList
    }

  def daily[F[_]](implicit F: Sync[F]): F[List[DailyResult]] =
    F.interruptible {
      dataset
        .map(m => dayResolution(m.localDateTime(zoneId)))
        .groupByKey(identity)
        .mapGroups((m, iter) => DailyResult(m, iter.size))
        .orderBy("date")
        .collect()
        .toList
    }

  def dailyHour[F[_]](implicit F: Sync[F]): F[List[DailyHourResult]] =
    F.interruptible {
      dataset
        .map(m => hourResolution(m.localDateTime(zoneId)).toString)
        .groupByKey(identity)
        .mapGroups((m, iter) => DailyHourResult(m, iter.size))
        .orderBy("dateTime")
        .collect()
        .toList
    }

  def dailyMinute[F[_]](implicit F: Sync[F]): F[List[DailyMinuteResult]] =
    F.interruptible {
      dataset
        .map(m => minuteResolution(m.localDateTime(zoneId)).toString)
        .groupByKey(identity)
        .mapGroups((m, iter) => DailyMinuteResult(m, iter.size))
        .orderBy("dateTime")
        .collect()
        .toList
    }

  private def internalSummary(ids: Dataset[CRMetaInfo]): List[KafkaSummaryInternal] = {
    import org.apache.spark.sql.functions.*
    ids
      .groupBy("partition")
      .agg(
        min("offset").as("startOffset"),
        max("offset").as("endOffset"),
        count(lit(1)).as("count"),
        min("timestamp").as("startTs"),
        max("timestamp").as("endTs"),
        first("topic").as("topic")
      )
      .as[KafkaSummaryInternal]
      .orderBy(asc("partition"))
      .collect()
      .toList
  }

  def summary[F[_]](implicit F: Sync[F]): F[List[KafkaSummary]] =
    F.interruptible(internalSummary(dataset).map(_.toKafkaSummary(zoneId)))

  /** Notes: offset is supposed to be monotonically increasing in a partition, except compact topic
    */
  def lostOffsets[F[_]](implicit F: Sync[F]): F[Dataset[MissingOffset]] = {
    import org.apache.spark.sql.functions.col
    F.interruptible {
      val all: List[Dataset[MissingOffset]] = internalSummary(dataset).map { kds =>
        val expect: Dataset[Long] =
          dataset.sparkSession.range(kds.startOffset, kds.endOffset + 1L).map(_.toLong)
        val exist: Dataset[Long] = dataset.filter(_.partition === kds.partition).map(_.offset)
        expect.except(exist).map(os => MissingOffset(partition = kds.partition, offset = os))
      }
      all
        .foldLeft(dataset.sparkSession.emptyDataset[MissingOffset])(_.union(_))
        .orderBy(col("partition").asc, col("offset").asc)
    }
  }

  def lostEarliest[F[_]: Sync]: F[List[ZonedCRMetaInfo]] =
    lostOffsets[F].map { (mo: Dataset[MissingOffset]) =>
      import org.apache.spark.sql.functions.min

      val ds: Dataset[(Int, Long)] =
        mo.groupBy("partition").agg(min("offset").as("offset")).as[(Int, Long)]

      ds.joinWith(dataset, ds("offset") - 1 === dataset("offset") && ds("partition") === dataset("partition"))
        .collect()
        .map(_._2.zoned(zoneId))
        .toList
        .sortBy(_.partition)
    }

  def lostLatest[F[_]: Sync]: F[List[ZonedCRMetaInfo]] =
    lostOffsets[F].map { (mo: Dataset[MissingOffset]) =>
      import org.apache.spark.sql.functions.max

      val ds: Dataset[(Int, Long)] =
        mo.groupBy("partition").agg(max("offset").as("offset")).as[(Int, Long)]

      ds.joinWith(dataset, ds("offset") + 1 === dataset("offset") && ds("partition") === dataset("partition"))
        .collect()
        .map(_._2.zoned(zoneId))
        .toList
        .sortBy(_.partition)
    }

  /** Notes:
    *
    * Timestamp is supposed to be ordered along with offset
    */
  def disorders[F[_]](implicit F: Sync[F]): F[Dataset[Disorder]] =
    F.interruptible {
      import org.apache.spark.sql.functions.col

      val all: Array[Dataset[Disorder]] =
        dataset.map(_.partition).distinct().collect().map { pt =>
          val curr: Dataset[(Long, CRMetaInfo)] = dataset.filter(_.partition === pt).map(x => (x.offset, x))
          val pre: Dataset[(Long, CRMetaInfo)]  = curr.map { case (index, crm) => (index + 1, crm) }

          curr.joinWith(pre, curr("_1") === pre("_1"), "inner").flatMap { case ((_, c), (_, p)) =>
            if (c.timestamp >= p.timestamp) None
            else
              Some(Disorder(
                partition = pt,
                offset = p.offset,
                timestamp = p.timestamp,
                currTs = p.localDateTime(zoneId).toString,
                nextTS = c.localDateTime(zoneId).toString,
                msGap = p.timestamp - c.timestamp,
                tsType = p.timestampType
              ))
          }
        }
      all
        .foldLeft(dataset.sparkSession.emptyDataset[Disorder])(_.union(_))
        .orderBy(col("partition").asc, col("offset").asc)
    }

  /** Notes: partition + offset supposed to be unique, of a topic
    */
  def dupRecords[F[_]](implicit F: Sync[F]): F[Dataset[DuplicateRecord]] = {
    import org.apache.spark.sql.functions.{asc, col, count, lit}
    F.interruptible {
      dataset
        .groupBy(col("partition"), col("offset"))
        .agg(count(lit(1)))
        .as[(Int, Long, Long)]
        .flatMap { case (p, o, c) =>
          if (c > 1) Some(DuplicateRecord(p, o, c)) else None
        }
        .orderBy(asc("partition"), asc("offset"))
    }
  }
}
