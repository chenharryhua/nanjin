package com.github.chenharryhua.nanjin.guard.observers

import cats.data.Reader
import cats.effect.kernel.{Async, Resource}
import cats.implicits.{catsSyntaxApplicative, catsSyntaxApplicativeError, toFunctorOps, toTraverseOps}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.{ses, EmailContent, SimpleEmailService}
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.{Pipe, Stream}
import org.typelevel.cats.time.instances.all
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scalatags.Text
import scalatags.Text.all.*

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.{DurationInt, FiniteDuration}
object email {

  def apply[F[_]: Async](
    from: String,
    to: List[String],
    subject: String,
    client: Resource[F, SimpleEmailService[F]]): NJEmail[F] =
    new NJEmail[F](
      from = from,
      to = to,
      subject = subject,
      client = client,
      chunkSize = 60,
      interval = 60.minutes,
      handlePassThrough = Reader(_ => div()),
      isLogging = false)

  def apply[F[_]: Async](from: String, to: List[String], subject: String): NJEmail[F] =
    apply[F](from, to, subject, ses[F])

}

final class NJEmail[F[_]: Async] private[observers] (
  from: String,
  to: List[String],
  subject: String,
  client: Resource[F, SimpleEmailService[F]],
  chunkSize: Int,
  interval: FiniteDuration,
  handlePassThrough: Reader[PassThrough, Text.TypedTag[String]],
  isLogging: Boolean
) extends Pipe[F, NJEvent, String] with all {

  private def copy(
    chunkSize: Int = chunkSize,
    interval: FiniteDuration = interval,
    handlePassThrough: Reader[PassThrough, Text.TypedTag[String]] = handlePassThrough,
    isLogging: Boolean = isLogging): NJEmail[F] =
    new NJEmail[F](from, to, subject, client, chunkSize, interval, handlePassThrough, isLogging)

  def withInterval(fd: FiniteDuration): NJEmail[F] = copy(interval = fd)
  def withChunkSize(cs: Int): NJEmail[F]           = copy(chunkSize = cs)
  def withLogging: NJEmail[F]                      = copy(isLogging = true)

  def withPassThrough(f: PassThrough => Text.TypedTag[String]): NJEmail[F] = copy(handlePassThrough = Reader(f))

  private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  override def apply(es: Stream[F, NJEvent]): Stream[F, String] =
    for {
      c <- Stream.resource(client)
      m <- es.groupWithin(chunkSize, interval).evalMap { events =>
        val mail = html(body(events.map(e => hr(transform(e))).toList)).render
        c.send(EmailContent(from, to.distinct, subject, mail)).attempt.as(mail)
      }
      _ <- Stream.eval(logger.info("\n" + m).whenA(isLogging))
    } yield m

  private def showTimestamp(timestamp: ZonedDateTime): String      = timestamp.truncatedTo(ChronoUnit.SECONDS).show
  private val fmt: DurationFormatter                               = DurationFormatter.defaultFormatter
  private def took(from: ZonedDateTime, to: ZonedDateTime): String = fmt.format(from, to)

  private def hostService(si: ServiceInfo): Text.TypedTag[String] =
    p(b("Service: "), si.serviceParams.serviceName, " ", b("Host: "), si.serviceParams.taskParams.hostName)

  def transform(event: NJEvent): Text.TypedTag[String] =
    event match {
      case ServiceStarted(serviceInfo, timestamp) =>
        div(h3(s"Service Started - ${showTimestamp(timestamp)}"), hostService(serviceInfo))
      case ServicePanic(serviceInfo, timestamp, retryDetails, error) =>
        div(
          h3(s"Service Panic - ${timestamp.truncatedTo(ChronoUnit.SECONDS).show}"),
          hostService(serviceInfo),
          p(b("Retries so far"), retryDetails.retriesSoFar),
          p(b("Cause")),
          pre(error.stackTrace)
        )
      case ServiceStopped(serviceInfo, timestamp, snapshot) =>
        div(
          h3(s"Service Stopped - ${showTimestamp(timestamp)}"),
          hostService(serviceInfo),
          pre(snapshot.show)
        )
      case MetricsReport(reportType, serviceInfo, timestamp, snapshot) =>
        div(
          h3(s"${reportType.show} - ${showTimestamp(timestamp)}"),
          hostService(serviceInfo),
          pre(snapshot.show)
        )
      case MetricsReset(resetType, serviceInfo, timestamp, snapshot) =>
        div(
          h3(s"${resetType.show} - ${showTimestamp(timestamp)}"),
          hostService(serviceInfo),
          pre(snapshot.show)
        )
      case ServiceAlert(metricName, serviceInfo, timestamp, importance, message) =>
        div(
          h3(s"Service Alert - ${showTimestamp(timestamp)}"),
          hostService(serviceInfo),
          p(b("Name: "), metricName.value, b("Importance: "), importance.show),
          pre(message)
        )

      case ActionFailed(actionInfo, timestamp, numRetries, notes, error) =>
        div(
          h3(s"Action Failed - ${showTimestamp(timestamp)}"),
          hostService(actionInfo.serviceInfo),
          p(b(s"${actionInfo.actionParams.alias}: "), actionInfo.actionParams.metricName.value),
          p(b(s"${actionInfo.actionParams.alias} ID: "), actionInfo.uuid.show),
          p(b("error ID: "), error.uuid.show),
          p(b("policy: "), actionInfo.actionParams.retry.policy[F].show),
          p(b("Took: "), took(actionInfo.launchTime, timestamp)),
          p(b("Number of Retries: "), numRetries.toString),
          pre(notes.value),
          p(b("Cause: ")),
          pre(error.stackTrace)
        )

      case ActionSucced(actionInfo, timestamp, numRetries, notes) =>
        div(
          h3(s"Action Succed - ${showTimestamp(timestamp)}"),
          hostService(actionInfo.serviceInfo),
          p(b(s"${actionInfo.actionParams.alias}: "), actionInfo.actionParams.metricName.value),
          p(b(s"${actionInfo.actionParams.alias} ID: "), actionInfo.uuid.show),
          p(b("Took: "), took(actionInfo.launchTime, timestamp)),
          p(b("Number of Retries: "), numRetries.toString),
          pre(notes.value)
        )

      case _: ActionStart    => div()
      case _: ActionRetrying => div()
      case pt: PassThrough   => handlePassThrough(pt)
    }
}
