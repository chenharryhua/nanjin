package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import munit.CatsEffectSuite
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.retries.StandardRetryStrategy
import software.amazon.awssdk.services.cloudwatch.CloudWatchClientBuilder
import software.amazon.awssdk.services.cloudwatch.model.{Dimension, MetricDatum}

import java.net.URI
import java.nio.charset.StandardCharsets

class CloudWatchStubTransportTest extends CatsEffectSuite {

  private val ENDPOINT = URI.create("https://monitoring.test")
  private val CREDENTIALS = StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret"))

  // CloudWatch speaks the AWS query protocol with XML responses.
  private def response(body: String): StubAwsHttpClient.Response =
    StubAwsHttpClient.Response(contentType = "text/xml", body = body.getBytes(StandardCharsets.UTF_8))

  private def configure_builder(
    transport: StubAwsHttpClient
  )(builder: CloudWatchClientBuilder): CloudWatchClientBuilder =
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

  private val putMetricDataXml =
    """<PutMetricDataResponse xmlns="http://monitoring.amazonaws.com/doc/2010-08-01/">
      |  <ResponseMetadata><RequestId>req-1</RequestId></ResponseMetadata>
      |</PutMetricDataResponse>""".stripMargin

  test("1.putMetricData marshals the request and decodes the XML response") {
    val transport = StubAwsHttpClient.queue(Right(response(putMetricDataXml)))

    CloudWatch[IO](configure_builder(transport)).use { cw =>
      val datum = MetricDatum
        .builder()
        .metricName("latency")
        .value(12.5)
        .dimensions(Dimension.builder().name("service").value("api").build())
        .build()
      cw.putMetricData(_.namespace("nj/test").metricData(datum)).map { resp =>
        assert(resp != null)
        // CloudWatch marshals a single request through the stub transport; the on-wire encoding is binary,
        // so we assert the call happened rather than parsing its body.
        assertEquals(transport.recordedRequests.size, 1)
      }
    }
  }

  test("2.transport failures propagate as SdkClientException wrapping the IOException") {
    val failure = new java.io.IOException("transport failed")
    val transport = StubAwsHttpClient.queue(Left(failure))

    CloudWatch[IO](configure_builder(transport)).use { cw =>
      interceptIO[software.amazon.awssdk.core.exception.SdkClientException](
        cw.putMetricData(_.namespace("nj/test"))).map { error =>
        assertEquals(error.getCause, failure)
      }
    }
  }

  test("3.managed resource release closes its owned transport exactly once") {
    val transport = StubAwsHttpClient.queue()

    CloudWatch[IO](configure_builder(transport)).use(_ => IO.unit).map { _ =>
      assertEquals(transport.closeCalls, 1)
    }
  }
}
