package com.github.chenharryhua.nanjin.spark.kafka

import cats.Endo
import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.{dayResolution, hourResolution, minuteResolution}
import com.github.chenharryhua.nanjin.messages.kafka.{CRMetaInfo, ZonedCRMetaInfo}
import com.github.chenharryhua.nanjin.spark.utils
import org.apache.spark.sql.Dataset

import java.time.ZoneId

final class Statistics[F[_]] private[spark] (val fdataset: F[Dataset[CRMetaInfo]])(implicit F: Sync[F])
    extends Serializable {

  def cherryPick(partition: Int, offset: Long): F[List[ZonedCRMetaInfo]] =
    fdataset.map { ds =>
      val zoneId: ZoneId = utils.sparkZoneId(ds.sparkSession)
      ds.filter(m => m.offset === offset && m.partition === partition).collect().toList.map(_.zoned(zoneId))
    }

  def transform(f: Endo[Dataset[CRMetaInfo]]): Statistics[F] =
    new Statistics[F](fdataset.map(f))

  def union(other: Statistics[F]): Statistics[F] =
    new Statistics[F](for {
      me <- fdataset
      you <- other.fdataset
    } yield me.union(you))

  def minutely: F[List[MinutelyAggResult]] = F.flatMap(fdataset) { ds =>
    import ds.sparkSession.implicits.*
    val zoneId: ZoneId = utils.sparkZoneId(ds.sparkSession)
    F.interruptible(
      ds.map(m => m.localDateTime(zoneId).getMinute)
        .groupByKey(identity)
        .mapGroups((m, iter) => MinutelyAggResult(m, iter.size))
        .orderBy("minute")
        .collect()
        .toList)
  }

  def hourly: F[List[HourlyAggResult]] = F.flatMap(fdataset) { ds =>
    import ds.sparkSession.implicits.*
    val zoneId: ZoneId = utils.sparkZoneId(ds.sparkSession)
    F.interruptible(
      ds.map(m => m.localDateTime(zoneId).getHour)
        .groupByKey(identity)
        .mapGroups((m, iter) => HourlyAggResult(m, iter.size))
        .orderBy("hour")
        .collect()
        .toList)
  }

  def daily: F[List[DailyAggResult]] = F.flatMap(fdataset) { ds =>
    import ds.sparkSession.implicits.*
    val zoneId: ZoneId = utils.sparkZoneId(ds.sparkSession)
    F.interruptible(
      ds.map(m => dayResolution(m.localDateTime(zoneId)))
        .groupByKey(identity)
        .mapGroups((m, iter) => DailyAggResult(m, iter.size))
        .orderBy("date")
        .collect()
        .toList)
  }

  def dailyHour: F[List[DailyHourAggResult]] = F.flatMap(fdataset) { ds =>
    import ds.sparkSession.implicits.*
    val zoneId: ZoneId = utils.sparkZoneId(ds.sparkSession)
    F.interruptible(
      ds.map(m => hourResolution(m.localDateTime(zoneId)).toString)
        .groupByKey(identity)
        .mapGroups((m, iter) => DailyHourAggResult(m, iter.size))
        .orderBy("dateTime")
        .collect()
        .toList)
  }

  def dailyMinute: F[List[DailyMinuteAggResult]] = F.flatMap(fdataset) { ds =>
    import ds.sparkSession.implicits.*
    val zoneId: ZoneId = utils.sparkZoneId(ds.sparkSession)
    F.interruptible(
      ds.map(m => minuteResolution(m.localDateTime(zoneId)).toString)
        .groupByKey(identity)
        .mapGroups((m, iter) => DailyMinuteAggResult(m, iter.size))
        .orderBy("dateTime")
        .collect()
        .toList)
  }

  private def internalSummary(ids: Dataset[CRMetaInfo]): List[KafkaSummaryInternal] = {
    import ids.sparkSession.implicits.*
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

  def summary: F[List[KafkaSummary]] = F.flatMap(fdataset) { ds =>
    val zoneId: ZoneId = utils.sparkZoneId(ds.sparkSession)
    F.interruptible(internalSummary(ds).map(_.toKafkaSummary(zoneId)))
  }

  /** Notes: offset is supposed to be monotonically increasing in a partition, except compact topic
    */
  def missingOffsets: F[Dataset[MissingOffset]] = F.flatMap(fdataset) { ds =>
    import ds.sparkSession.implicits.*
    import org.apache.spark.sql.functions.col
    F.interruptible {
      val all: List[Dataset[MissingOffset]] = internalSummary(ds).map { kds =>
        val expect: Dataset[Long] = ds.sparkSession.range(kds.startOffset, kds.endOffset + 1L).map(_.toLong)
        val exist: Dataset[Long]  = ds.filter(_.partition === kds.partition).map(_.offset)
        expect.except(exist).map(os => MissingOffset(partition = kds.partition, offset = os))
      }
      all
        .foldLeft(ds.sparkSession.emptyDataset[MissingOffset])(_.union(_))
        .orderBy(col("partition").asc, col("offset").asc)
    }
  }

  /** Notes:
    *
    * Timestamp is supposed to be ordered along with offset
    */
  def disorders: F[Dataset[Disorder]] = F.flatMap(fdataset) { ds =>
    import ds.sparkSession.implicits.*
    import org.apache.spark.sql.functions.col
    F.interruptible {
      val zoneId: ZoneId = utils.sparkZoneId(ds.sparkSession)
      val all: Array[Dataset[Disorder]] =
        ds.map(_.partition).distinct().collect().map { pt =>
          val curr: Dataset[(Long, CRMetaInfo)] = ds.filter(_.partition === pt).map(x => (x.offset, x))
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
        .foldLeft(ds.sparkSession.emptyDataset[Disorder])(_.union(_))
        .orderBy(col("partition").asc, col("offset").asc)
    }
  }

  /** Notes: partition + offset supposed to be unique, of a topic
    */
  def dupRecords: F[Dataset[DuplicateRecord]] = F.flatMap(fdataset) { ds =>
    import ds.sparkSession.implicits.*
    import org.apache.spark.sql.functions.{asc, col, count, lit}
    F.interruptible(
      ds.groupBy(col("partition"), col("offset"))
        .agg(count(lit(1)))
        .as[(Int, Long, Long)]
        .flatMap { case (p, o, c) =>
          if (c > 1) Some(DuplicateRecord(p, o, c)) else None
        }
        .orderBy(asc("partition"), asc("offset")))
  }
}
