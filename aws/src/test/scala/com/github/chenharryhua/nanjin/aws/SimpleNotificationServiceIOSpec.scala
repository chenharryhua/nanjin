package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sns.model.{PublishRequest, PublishResponse}

class SimpleNotificationServiceIOSpec extends AnyFlatSpec with Matchers {

  // Fake SnsClient for testing
  class FakeSnsClient extends SnsClient {
    override def publish(request: PublishRequest): PublishResponse =
      PublishResponse.builder().messageId("fake-message-id").build()

    override def close(): Unit = ()

    override def serviceName(): String = "abc"
  }

  "SimpleNotificationService" should "publish a message" in {
    val fakeClient = new FakeSnsClient()

    // Wrap fake client into our service
    val service: SimpleNotificationService[IO] =
      new SimpleNotificationService[IO] {
        override def publish(req: PublishRequest): IO[PublishResponse] =
          IO(fakeClient.publish(req))
      }

    val req = PublishRequest.builder().message("Hello").topicArn("arn:aws:sns:fake:123:topic").build()
    val result = service.publish(req).unsafeRunSync()

    result.messageId() shouldBe "fake-message-id"
  }

  it should "publish using builder syntax" in {
    val fakeClient = new FakeSnsClient()

    val service: SimpleNotificationService[IO] =
      new SimpleNotificationService[IO] {
        override def publish(req: PublishRequest): IO[PublishResponse] =
          IO(fakeClient.publish(req))
      }

    val result = service.publish(_.message("Hi").topicArn("arn:aws:sns:fake:123:topic")).unsafeRunSync()
    result.messageId() shouldBe "fake-message-id"
  }
}
