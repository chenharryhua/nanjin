package com.github.chenharryhua.nanjin.kafka

import java.time.{LocalDate, LocalDateTime}

import cats.effect.{Async, ContextShift, Effect, IO, LiftIO, Resource}
import cats.implicits._

trait KafkaStatistics[F[_]] {
  private def now: LocalDateTime         = LocalDateTime.now
  private def tenYearsAgo: LocalDateTime = now.minusYears(10)

  def ymd(start: LocalDateTime, end: LocalDateTime): F[Map[LocalDate, Long]]
  def ymd(date: LocalDateTime): F[Map[LocalDate, Long]] = ymd(date, now)
  def ymd: F[Map[LocalDate, Long]]                      = ymd(tenYearsAgo, now)

  def daily(start: LocalDateTime, end: LocalDateTime): F[Map[Int, Long]]
  def daily(date: LocalDateTime): F[Map[Int, Long]] = daily(date, now)
  def daily: F[Map[Int, Long]]                      = daily(now, tenYearsAgo)

  def hourly(start: LocalDateTime, end: LocalDateTime): F[Map[Int, Long]]
  def hourly(date: LocalDateTime): F[Map[Int, Long]] = hourly(date, LocalDateTime.now)
  def hourly: F[Map[Int, Long]]                      = hourly(tenYearsAgo, now)

  def minutely(start: LocalDateTime, end: LocalDateTime): F[Map[Int, Long]]
  def minutely(date: LocalDateTime): F[Map[Int, Long]] = minutely(date, LocalDateTime.now)
  def minutely: F[Map[Int, Long]]                      = minutely(tenYearsAgo, now)
}

object KafkaStatistics {

  def apply[F[_]: Effect: ContextShift, K, V](
    consumer: KafkaConsumerApi[F, K, V],
    channel: Resource[F, KafkaChannels.AkkaChannel[F, K, V]]): KafkaStatistics[F] =
    new StatisticsImpl[F, K, V](consumer, channel)

  final private class StatisticsImpl[F[_]: Effect: ContextShift, K, V](
    consumer: KafkaConsumerApi[F, K, V],
    channel: Resource[F, KafkaChannels.AkkaChannel[F, K, V]])
      extends KafkaStatistics[F] {

    private[this] def statistics[MK](
      start: LocalDateTime,
      end: LocalDateTime,
      f: LocalDateTime => MK): F[Map[MK, Long]] =
      for {
        tps <- consumer.offsetRangeFor(start, end)
        beginnings = tps.mapValues(_.fromOffset)
        endings    = tps.mapValues(_.untilOffset)
        size       = tps.value.map { case (_, v) => v.size }.sum
        ret <- channel.use { chn =>
          val fut = chn
            .assign(beginnings.value)
            .groupBy(10, _.partition())
            .takeWhile(p => endings.get(p.topic, p.partition).exists(p.offset < _))
            .mergeSubstreams
            .take(size)
            .runFold(Map[MK, Long]()) {
              case (sum, c) =>
                val key = f(utils.kafkaTimestamp2LocalDateTime(c.timestamp()))
                sum.get(key) match {
                  case None    => sum + (key -> 1)
                  case Some(x) => sum + (key -> (x + 1))
                }
            }(chn.materializer)
          Async.fromFuture(Async[F].pure(fut))
        }
      } yield ret

    override def daily(start: LocalDateTime, end: LocalDateTime): F[Map[Int, Long]] =
      statistics(start, end, _.getDayOfMonth)

    override def ymd(start: LocalDateTime, end: LocalDateTime): F[Map[LocalDate, Long]] =
      statistics(start, end, _.toLocalDate)

    override def hourly(start: LocalDateTime, end: LocalDateTime): F[Map[Int, Long]] =
      statistics(start, end, _.getHour)

    override def minutely(start: LocalDateTime, end: LocalDateTime): F[Map[Int, Long]] =
      statistics(start, end, _.getMinute)
  }
}
