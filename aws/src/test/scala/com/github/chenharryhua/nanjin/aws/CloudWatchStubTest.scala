package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import cats.effect.kernel.Sync
import munit.CatsEffectSuite
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient
import software.amazon.awssdk.services.cloudwatch.model.{PutMetricDataRequest, PutMetricDataResponse}

class CloudWatchStubTest extends CatsEffectSuite {

  /** Dummy CloudWatchClient that always returns a successful response */
  private val dummyClient = new CloudWatchClient {
    override def putMetricData(request: PutMetricDataRequest): PutMetricDataResponse =
      PutMetricDataResponse.builder().build()
    // other methods can throw UnsupportedOperationException
    override def close(): Unit = ()

    override def serviceName(): String = "abc"
  }

  /** CloudWatch[F] using a stub client */
  private def stubCloudWatch[F[_]: Sync]: CloudWatch[F] =
    new CloudWatch[F] {
      override def putMetricData(request: PutMetricDataRequest): F[PutMetricDataResponse] =
        Sync[F].delay(dummyClient.putMetricData(request))
    }

  test("CloudWatch.putMetricData should return a response") {
    val cw = stubCloudWatch[IO]
    val request = PutMetricDataRequest.builder().namespace("test").build()
    cw.putMetricData(request).map { resp =>
      assert(resp != null)
    }
  }

  test("CloudWatch.putMetricData using builder function") {
    val cw = stubCloudWatch[IO]
    val requestBuilder = (b: PutMetricDataRequest.Builder) => b.namespace("builderTest")
    cw.putMetricData(requestBuilder).map { resp =>
      assert(resp != null)
    }
  }
}
