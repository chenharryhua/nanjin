package com.github.chenharryhua.nanjin.aws

import cats.data.NonEmptyList
import cats.effect.IO
import munit.CatsEffectSuite
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.retries.StandardRetryStrategy
import software.amazon.awssdk.services.ses.SesClientBuilder
import software.amazon.awssdk.services.ses.model.{RawMessage, SendRawEmailRequest}

import java.net.URI
import java.nio.charset.StandardCharsets

class SimpleEmailServiceStubTest extends CatsEffectSuite {

  private val ENDPOINT = URI.create("https://ses.test")
  private val CREDENTIALS = StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret"))

  // SES v1 speaks the AWS query protocol with XML responses.
  private def response(body: String): StubAwsHttpClient.Response =
    StubAwsHttpClient.Response(contentType = "text/xml", body = body.getBytes(StandardCharsets.UTF_8))

  private def configure_builder(
    transport: StubAwsHttpClient
  )(builder: SesClientBuilder): SesClientBuilder =
    builder
      .httpClientBuilder(transport.builder)
      .credentialsProvider(CREDENTIALS)
      .region(Region.US_EAST_1)
      .endpointOverride(ENDPOINT)
      .overrideConfiguration(
        ClientOverrideConfiguration
          .builder()
          .retryStrategy(StandardRetryStrategy.builder().maxAttempts(1).build())
          .build())

  private val sendEmailXml =
    """<SendEmailResponse xmlns="http://ses.amazonaws.com/doc/2010-12-01/">
      |  <SendEmailResult><MessageId>email-1</MessageId></SendEmailResult>
      |  <ResponseMetadata><RequestId>req-1</RequestId></ResponseMetadata>
      |</SendEmailResponse>""".stripMargin

  private val sendRawEmailXml =
    """<SendRawEmailResponse xmlns="http://ses.amazonaws.com/doc/2010-12-01/">
      |  <SendRawEmailResult><MessageId>raw-1</MessageId></SendRawEmailResult>
      |  <ResponseMetadata><RequestId>req-2</RequestId></ResponseMetadata>
      |</SendRawEmailResponse>""".stripMargin

  test("1.send(EmailContent) marshals destinations and decodes the XML response") {
    val transport = StubAwsHttpClient.queue(Right(response(sendEmailXml)))

    val content = EmailContent(
      from = Email("from@example.com"),
      to = NonEmptyList.of(Email("to@example.com")),
      subject = "Subject",
      body = "<h1>hi</h1>",
      cc = List(Email("cc@example.com")),
      bcc = List(Email("bcc@example.com"))
    )

    SimpleEmailService[IO](configure_builder(transport)).use { ses =>
      ses.send(content).map { resp =>
        assertEquals(resp.messageId(), "email-1")
        val body = transport.recordedRequests.head.bodyUtf8
        assert(body.contains("Action=SendEmail"), body)
        assert(body.contains("Source=from%40example.com"), body)
      }
    }
  }

  test("2.send(Endo builder) decodes the XML response") {
    val transport = StubAwsHttpClient.queue(Right(response(sendEmailXml)))

    SimpleEmailService[IO](configure_builder(transport)).use { ses =>
      ses.send(_.source("from@example.com")).map(resp => assertEquals(resp.messageId(), "email-1"))
    }
  }

  test("3.send(SendRawEmailRequest) decodes the XML response") {
    val transport = StubAwsHttpClient.queue(Right(response(sendRawEmailXml)))

    val request = SendRawEmailRequest
      .builder()
      .rawMessage(RawMessage.builder().data(SdkBytes.fromUtf8String("From: a@b.c\n\nbody")).build())
      .build()

    SimpleEmailService[IO](configure_builder(transport)).use { ses =>
      ses.send(request).map(resp => assertEquals(resp.messageId(), "raw-1"))
    }
  }

  test("4.transport failures propagate as SdkClientException wrapping the IOException") {
    val failure = new java.io.IOException("transport failed")
    val transport = StubAwsHttpClient.queue(Left(failure))

    SimpleEmailService[IO](configure_builder(transport)).use { ses =>
      interceptIO[software.amazon.awssdk.core.exception.SdkClientException](
        ses.send(_.source("from@example.com"))).map { error =>
        assertEquals(error.getCause, failure)
      }
    }
  }

  test("5.managed resource release closes its owned transport exactly once") {
    val transport = StubAwsHttpClient.queue()

    SimpleEmailService[IO](configure_builder(transport)).use(_ => IO.unit).map { _ =>
      assertEquals(transport.closeCalls, 1)
    }
  }
}
