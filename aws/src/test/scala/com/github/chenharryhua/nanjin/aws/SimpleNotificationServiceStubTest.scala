package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import munit.CatsEffectSuite
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.retries.StandardRetryStrategy
import software.amazon.awssdk.services.sns.SnsClientBuilder

import java.net.URI
import java.nio.charset.StandardCharsets

class SimpleNotificationServiceStubTest extends CatsEffectSuite {

  private val ENDPOINT = URI.create("https://sns.test")
  private val CREDENTIALS = StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret"))
  private val TOPIC = "arn:aws:sns:us-east-1:123456789012:MyTopic"

  // SNS speaks the AWS query protocol with XML responses.
  private def response(body: String): StubAwsHttpClient.Response =
    StubAwsHttpClient.Response(contentType = "text/xml", body = body.getBytes(StandardCharsets.UTF_8))

  private def configure_builder(
    transport: StubAwsHttpClient
  )(builder: SnsClientBuilder): SnsClientBuilder =
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

  private val publishXml =
    """<PublishResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
      |  <PublishResult><MessageId>msg-123</MessageId></PublishResult>
      |  <ResponseMetadata><RequestId>req-1</RequestId></ResponseMetadata>
      |</PublishResponse>""".stripMargin

  test("1.publish marshals the request and decodes the XML response") {
    val transport = StubAwsHttpClient.queue(Right(response(publishXml)))

    SimpleNotificationService[IO](configure_builder(transport)).use { sns =>
      sns.publish(_.topicArn(TOPIC).message("Hello, SNS!")).map { resp =>
        assertEquals(resp.messageId(), "msg-123")
        val body = transport.recordedRequests.head.bodyUtf8
        assert(body.contains("Action=Publish"), body)
        assert(body.contains("Message=Hello%2C+SNS%21"), body)
      }
    }
  }

  test("2.publish using a fully built request decodes the XML response") {
    val transport = StubAwsHttpClient.queue(Right(response(publishXml)))

    SimpleNotificationService[IO](configure_builder(transport)).use { sns =>
      val request = software.amazon.awssdk.services.sns.model.PublishRequest
        .builder()
        .topicArn(TOPIC)
        .message("body")
        .build()
      sns.publish(request).map(resp => assertEquals(resp.messageId(), "msg-123"))
    }
  }

  test("3.transport failures propagate as SdkClientException wrapping the IOException") {
    val failure = new java.io.IOException("transport failed")
    val transport = StubAwsHttpClient.queue(Left(failure))

    SimpleNotificationService[IO](configure_builder(transport)).use { sns =>
      interceptIO[software.amazon.awssdk.core.exception.SdkClientException](
        sns.publish(_.topicArn(TOPIC).message("x"))).map { error =>
        assertEquals(error.getCause, failure)
      }
    }
  }

  test("4.managed resource release closes its owned transport exactly once") {
    val transport = StubAwsHttpClient.queue()

    SimpleNotificationService[IO](configure_builder(transport)).use(_ => IO.unit).map { _ =>
      assertEquals(transport.closeCalls, 1)
    }
  }
}
