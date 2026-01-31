package com.github.chenharryhua.nanjin.aws

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.aws.EmailContent
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.ses.SesClient
import software.amazon.awssdk.services.ses.model.*

class SimpleEmailServiceIOSpec extends AnyFunSuite {

  // Fake SES client for testing
  private class FakeSesClient extends SesClient {
    override def sendEmail(req: SendEmailRequest): SendEmailResponse =
      SendEmailResponse.builder().messageId(s"fake-${req.toString}").build()

    override def sendRawEmail(req: SendRawEmailRequest): SendRawEmailResponse =
      SendRawEmailResponse.builder().messageId(s"raw-fake-${req.toString}").build()

    override def close(): Unit = ()

    override def serviceName(): String = "abc"
  }

  // Helper to create SimpleEmailService from a fake client
  private object SimpleEmailService {
    def fromClient(client: SesClient): SimpleEmailService[IO] =
      new SimpleEmailService[IO] {
        override def send(req: SendEmailRequest): IO[SendEmailResponse] =
          IO(client.sendEmail(req))

        override def send(req: SendRawEmailRequest): IO[SendRawEmailResponse] =
          IO(client.sendRawEmail(req))
      }
  }

  private val service: SimpleEmailService[IO] =
    SimpleEmailService.fromClient(new FakeSesClient())

  test("send SendEmailRequest returns fake messageId") {
    val req = SendEmailRequest
      .builder()
      .source("from@example.com")
      .destination(Destination.builder().toAddresses("to@example.com").build())
      .message(
        Message
          .builder()
          .subject(Content.builder().data("subject").build())
          .body(Body.builder().text(Content.builder().data("body").build()).build())
          .build()
      )
      .build()

    val resp = service.send(req).unsafeRunSync()
    assert(resp.messageId().startsWith("fake-"))
  }

  test("send SendRawEmailRequest returns fake messageId") {
    val req = SendRawEmailRequest
      .builder()
      .rawMessage(RawMessage.builder().data(SdkBytes.fromByteArray("data".getBytes)).build())
      .build()

    val resp = service.send(req).unsafeRunSync()
    assert(resp.messageId().startsWith("raw-fake-"))
  }

  test("send EmailContent returns SendEmailResponse with fake messageId") {
    val content = EmailContent(
      from = "from@example.com",
      to = NonEmptyList.one("to@example.com"),
      cc = Nil,
      bcc = Nil,
      subject = "subject",
      body = "<h1>body</h1>"
    )

    val resp = service.send(content).unsafeRunSync()
    assert(resp.messageId().startsWith("fake-"))
  }

  test("send builder function returns SendEmailResponse with fake messageId") {
    val resp = service.send(_.source("from@example.com")).unsafeRunSync()
    assert(resp.messageId().startsWith("fake-"))
  }
}
