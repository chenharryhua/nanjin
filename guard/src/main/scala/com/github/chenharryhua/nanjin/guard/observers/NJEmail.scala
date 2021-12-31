package com.github.chenharryhua.nanjin.guard.observers

import cats.data.Reader
import cats.effect.kernel.{Async, Resource}
import cats.implicits.{catsSyntaxApplicative, catsSyntaxApplicativeError, toFunctorOps, toTraverseOps}
import com.github.chenharryhua.nanjin.aws.{ses, EmailContent, SimpleEmailService}
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.translators.{DefaultEmailTranslator, Translator, UpdateTranslator}
import fs2.{Pipe, Stream}
import org.typelevel.cats.time.instances.all
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
    new NJEmail[F](
      from = from,
      to = to,
      subject = subject,
      client = client,
      chunkSize = 60,
      interval = 60.minutes,
      handlePassThrough = Reader(_ => div()),
      isLogging = false,
      DefaultEmailTranslator[F]()
    )

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
  isLogging: Boolean,
  translator: Translator[F, Text.TypedTag[String]]
) extends Pipe[F, NJEvent, String] with UpdateTranslator[F, Text.TypedTag[String], NJEmail[F]] with all {

  private def copy(
    chunkSize: Int = chunkSize,
    interval: FiniteDuration = interval,
    handlePassThrough: Reader[PassThrough, Text.TypedTag[String]] = handlePassThrough,
    isLogging: Boolean = isLogging,
    translator: Translator[F, Text.TypedTag[String]] = translator): NJEmail[F] =
    new NJEmail[F](from, to, subject, client, chunkSize, interval, handlePassThrough, isLogging, translator)

  def withInterval(fd: FiniteDuration): NJEmail[F] = copy(interval = fd)
  def withChunkSize(cs: Int): NJEmail[F]           = copy(chunkSize = cs)
  def withLogging: NJEmail[F]                      = copy(isLogging = true)

  private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  override def updateTranslator(
    f: Translator[F, Text.TypedTag[String]] => Translator[F, Text.TypedTag[String]]): NJEmail[F] =
    copy(translator = f(translator))

  override def apply(es: Stream[F, NJEvent]): Stream[F, String] =
    for {
      c <- Stream.resource(client)
      mb <- es.evalMap(e => translator.translate(e)).unNone.groupWithin(chunkSize, interval).evalMap { events =>
        val mailBody: String =
          html(body(events.map(hr(_)).toList, footer(hr(p(b("Total Events: "), events.size))))).render
        c.send(EmailContent(from, to.distinct, subject, mailBody)).attempt.as(mailBody)
      }
      _ <- Stream.eval(logger.info("\n\n" + mb + "\n\n").whenA(isLogging))
    } yield mb
}
