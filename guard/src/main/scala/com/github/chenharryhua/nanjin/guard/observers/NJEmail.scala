package com.github.chenharryhua.nanjin.guard.observers

import cats.data.Reader
import cats.effect.kernel.{Async, Resource}
import cats.implicits.{catsSyntaxApplicative, catsSyntaxApplicativeError, toFunctorOps, toTraverseOps}
import com.github.chenharryhua.nanjin.aws.{ses, EmailContent, SimpleEmailService}
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.{Pipe, Stream}
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scalatags.Text
import scalatags.Text.all.*

import scala.concurrent.duration.{DurationInt, FiniteDuration}
object email {

  def apply[F[_]: Async](
    from: String,
    to: List[String],
    subject: String,
    client: Resource[F, SimpleEmailService[F]]): NJEmail[F] =
    new NJEmail[F](from, to, subject, client, 60, 60.minutes, Reader(_ => div()), false)

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
) extends Pipe[F, NJEvent, String] {

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

  private def hostService(si: ServiceInfo): Text.TypedTag[String] =
    p(b("Service:"), si.serviceParams.serviceName, " ", b("Host:"), si.serviceParams.taskParams.hostName)

  def transform(event: NJEvent): Text.TypedTag[String] =
    event match {
      case ServiceStarted(serviceInfo, timestamp) =>
        div(h3("Service Started"), hostService(serviceInfo))
      case ServicePanic(serviceInfo, timestamp, retryDetails, error) =>
        div(
          h3("Service Panic"),
          hostService(serviceInfo),
          pre(error.stackTrace)
        )
      case ServiceStopped(serviceInfo, timestamp, snapshot) =>
        div(
          h3("Service Stopped"),
          hostService(serviceInfo),
          pre(snapshot.show)
        )
      case MetricsReport(reportType, serviceInfo, timestamp, snapshot) =>
        div(
          h3("Metrics Report"),
          hostService(serviceInfo),
          pre(snapshot.show)
        )
      case MetricsReset(resetType, serviceInfo, timestamp, snapshot) =>
        div(
          h3("Metrics Reset"),
          hostService(serviceInfo),
          pre(snapshot.show)
        )
      case ServiceAlert(metricName, serviceInfo, timestamp, importance, message) =>
        div(
          h3("Service Alert"),
          hostService(serviceInfo),
          pre(message)
        )

      case ActionStart(actionInfo) =>
        div(
          h3("Action Start"),
          hostService(actionInfo.serviceInfo),
          p("Kick off ", actionInfo.actionParams.alias),
          ": ",
          b(actionInfo.actionParams.metricName.value)
        )
      case ActionRetrying(actionInfo, timestamp, willDelayAndRetry, error) =>
        div(
          h3("Action Retrying"),
          hostService(actionInfo.serviceInfo),
          pre(error.stackTrace)
        )
      case ActionFailed(actionInfo, timestamp, numRetries, notes, error) =>
        div(
          h3("Action Failed"),
          hostService(actionInfo.serviceInfo),
          pre(error.stackTrace)
        )
      case ActionSucced(actionInfo, timestamp, numRetries, notes) =>
        div(
          h3("Action Succed"),
          hostService(actionInfo.serviceInfo),
          pre(notes.value)
        )

      case pt: PassThrough => handlePassThrough(pt)
    }
}
