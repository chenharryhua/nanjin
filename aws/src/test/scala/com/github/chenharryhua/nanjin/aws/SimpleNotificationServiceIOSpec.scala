package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import munit.CatsEffectSuite
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sns.model.{PublishRequest, PublishResponse}

class SimpleNotificationServiceIOSpec extends CatsEffectSuite {

  // Fake SnsClient for testing
  class FakeSnsClient extends SnsClient {
    override def publish(request: PublishRequest): PublishResponse =
      PublishResponse.builder().messageId("fake-message-id").build()

    override def close(): Unit = ()

    override def serviceName(): String = "abc"
  }

  test("SimpleNotificationService: publish a message") {
    val fakeClient = new FakeSnsClient()

    // Wrap fake client into our service
    val service: SimpleNotificationService[IO] =
      new SimpleNotificationService[IO] {
        override def publish(req: PublishRequest): IO[PublishResponse] =
          IO(fakeClient.publish(req))
      }

    val req = PublishRequest.builder().message("Hello").topicArn("arn:aws:sns:fake:123:topic").build()
    service.publish(req).map { result =>
      assertEquals(result.messageId(), "fake-message-id")
    }
  }

  test("SimpleNotificationService: publish using builder syntax") {
    val fakeClient = new FakeSnsClient()

    val service: SimpleNotificationService[IO] =
      new SimpleNotificationService[IO] {
        override def publish(req: PublishRequest): IO[PublishResponse] =
          IO(fakeClient.publish(req))
      }

    service.publish(_.message("Hi").topicArn("arn:aws:sns:fake:123:topic")).map { result =>
      assertEquals(result.messageId(), "fake-message-id")
    }
  }
}
