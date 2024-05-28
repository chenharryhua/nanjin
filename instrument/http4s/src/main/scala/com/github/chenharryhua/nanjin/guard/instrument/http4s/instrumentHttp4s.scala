package com.github.chenharryhua.nanjin.guard.instrument.http4s

import cats.effect.kernel.{Resource, Sync}
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.guard.service.Agent
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.http4s.metrics.{MetricsOps, TerminationType}
import org.http4s.{Method, Status}

import java.time.Duration
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

private case class TimeAndCount(time: Long, count: Long) {
  def update(other: TimeAndCount): TimeAndCount = TimeAndCount(time + other.time, count + 1)
}

final class NJHttp4sMetricsListener[F[_]](implicit F: Sync[F]) extends MetricsOps[F] {
  private val activeRequest: LongAdder =
    new LongAdder
  private val headersTime: ConcurrentMap[Method, TimeAndCount] =
    new ConcurrentHashMap[Method, TimeAndCount]
  private val totalTime: ConcurrentMap[Method, TimeAndCount] =
    new ConcurrentHashMap[Method, TimeAndCount]
  private val abnormalTerminal: ConcurrentMap[TerminationType, Long] =
    new ConcurrentHashMap[TerminationType, Long]

  override def increaseActiveRequests(classifier: Option[String]): F[Unit] =
    F.delay(activeRequest.increment())

  override def decreaseActiveRequests(classifier: Option[String]): F[Unit] =
    F.delay(activeRequest.decrement())

  override def recordHeadersTime(method: Method, elapsed: Long, classifier: Option[String]): F[Unit] =
    F.delay(headersTime.merge(method, TimeAndCount(elapsed, 1), _ update _)).void

  override def recordTotalTime(
    method: Method,
    status: Status,
    elapsed: Long,
    classifier: Option[String]): F[Unit] =
    F.delay(totalTime.merge(method, TimeAndCount(elapsed, 1), _ update _)).void

  override def recordAbnormalTermination(
    elapsed: Long,
    terminationType: TerminationType,
    classifier: Option[String]): F[Unit] =
    F.delay(abnormalTerminal.merge(terminationType, elapsed, _ + _)).void

  private def to_json_timer(cm: ConcurrentMap[Method, TimeAndCount]): Json = {
    val m = scala.collection.mutable.Map[String, Json]()
    cm.forEach { (k, v) =>
      m.update(k.name, Json.obj("spent" -> Duration.ofNanos(v.time).asJson, "count" -> v.count.asJson))
    }
    m.asJson
  }

  private def to_json_termination(cm: ConcurrentMap[TerminationType, Long]): Json = {
    val m = scala.collection.mutable.Map[String, Long]()
    cm.forEach { (k, v) =>
      val rename = k match {
        case TerminationType.Abnormal(_) => "abnormal"
        case TerminationType.Canceled    => "canceled"
        case TerminationType.Error(_)    => "error"
        case TerminationType.Timeout     => "timeout"
      }
      m.update(rename, v)
    }
    m.asJson
  }

  val snapshot: F[Json] =
    F.delay(
      Json.obj(
        "activeRequest" -> activeRequest.sum().asJson,
        "headersTime" -> to_json_timer(headersTime),
        "totalTime" -> to_json_timer(totalTime),
        "abnormalTerminal" -> to_json_termination(abnormalTerminal)
      ))
}

object instrumentHttp4s {
  def apply[F[_]](agent: Agent[F], listener: NJHttp4sMetricsListener[F], name: String): Resource[F, Unit] =
    agent.gauge(name).instrument(listener.snapshot)
}
