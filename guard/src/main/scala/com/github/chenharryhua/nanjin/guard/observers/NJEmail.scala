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

  private def timestampText(timestamp: ZonedDateTime): Text.TypedTag[String] =
    p(b("timestamp: "), timestamp.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show)
  private val fmt: DurationFormatter = DurationFormatter.defaultFormatter
  private def tookText(from: ZonedDateTime, to: ZonedDateTime): Text.TypedTag[String] =
    p(b("took: "), fmt.format(from, to))

  private def retriesText(numRetry: Int): Text.TypedTag[String] =
    p(b("number of retries: "), numRetry.toString)

  private def hostServiceText(si: ServiceInfo): Text.TypedTag[String] =
    p(b("service: "), si.serviceParams.metricName.value, " ", b("host: "), si.serviceParams.taskParams.hostName)

  private def notesText(n: Notes): Text.TypedTag[String]   = p(b("notes: "), pre(n.value))
  private def causeText(c: NJError): Text.TypedTag[String] = p(b("cause: "), pre(c.stackTrace))

  def transform(event: NJEvent): Text.TypedTag[String] =
    event match {
      case ServiceStarted(serviceInfo, timestamp) =>
        div(h3(s"Service Started"), timestampText(timestamp), hostServiceText(serviceInfo))
      case ServicePanic(serviceInfo, timestamp, retryDetails, error) =>
        div(
          h3(s"Service Panic"),
          timestampText(timestamp),
          hostServiceText(serviceInfo),
          p(b("restart so far: "), retryDetails.retriesSoFar),
          p(b("cause: ")),
          pre(error.stackTrace)
        )
      case ServiceStopped(serviceInfo, timestamp, snapshot) =>
        div(
          h3(s"Service Stopped"),
          timestampText(timestamp),
          hostServiceText(serviceInfo),
          pre(snapshot.show)
        )
      case MetricsReport(reportType, serviceInfo, _, snapshot) =>
        div(
          h3(reportType.show),
          hostServiceText(serviceInfo),
          pre(snapshot.show)
        )
      case MetricsReset(resetType, serviceInfo, _, snapshot) =>
        div(
          h3(resetType.show),
          hostServiceText(serviceInfo),
          pre(snapshot.show)
        )
      case ServiceAlert(metricName, serviceInfo, timestamp, importance, message) =>
        div(
          h3("Service Alert"),
          timestampText(timestamp),
          hostServiceText(serviceInfo),
          p(b("Name: "), metricName.value, b("Importance: "), importance.show),
          pre(message)
        )

      case ActionFailed(actionInfo, timestamp, numRetries, notes, error) =>
        div(
          h3(s"${actionInfo.actionParams.actionName} Failed"),
          timestampText(timestamp),
          hostServiceText(actionInfo.serviceInfo),
          p(b(s"${actionInfo.actionParams.alias} ID: "), actionInfo.uuid.show),
          p(b("error ID: "), error.uuid.show),
          p(b("policy: "), actionInfo.actionParams.retry.policy[F].show),
          tookText(actionInfo.launchTime, timestamp),
          retriesText(numRetries),
          notesText(notes),
          causeText(error)
        )

      case ActionSucced(actionInfo, timestamp, numRetries, notes) =>
        div(
          h3(s"${actionInfo.actionParams.actionName} Succed"),
          timestampText(timestamp),
          hostServiceText(actionInfo.serviceInfo),
          p(b(s"${actionInfo.actionParams.alias} ID: "), actionInfo.uuid.show),
          tookText(actionInfo.launchTime, timestamp),
          retriesText(numRetries),
          notesText(notes)
        )

      case _: ActionStart    => div()
      case _: ActionRetrying => div()
      case pt: PassThrough   => handlePassThrough(pt)
    }
}
