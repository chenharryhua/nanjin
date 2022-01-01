package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Async, Resource}
import cats.implicits.{catsSyntaxApplicative, catsSyntaxApplicativeError, toFunctorOps, toTraverseOps}
import com.github.chenharryhua.nanjin.aws.{ses, EmailContent, SimpleEmailService}
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.translators.{HtmlTranslator, Translator, UpdateTranslator}
import fs2.{Pipe, Stream}
import org.typelevel.cats.time.instances.all
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
      HtmlTranslator[F]()
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
  translator: Translator[F, Text.TypedTag[String]]
) extends Pipe[F, NJEvent, String] with UpdateTranslator[F, Text.TypedTag[String], NJEmail[F]] with all {

  private def copy(
    chunkSize: Int = chunkSize,
    interval: FiniteDuration = interval,
    translator: Translator[F, Text.TypedTag[String]] = translator): NJEmail[F] =
    new NJEmail[F](from, to, subject, client, chunkSize, interval, translator)

  def withInterval(fd: FiniteDuration): NJEmail[F] = copy(interval = fd)
  def withChunkSize(cs: Int): NJEmail[F]           = copy(chunkSize = cs)

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
    } yield mb
}
