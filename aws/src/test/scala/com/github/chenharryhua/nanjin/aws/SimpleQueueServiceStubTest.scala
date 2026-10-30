package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.Policy
import munit.CatsEffectSuite
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.retries.StandardRetryStrategy
import software.amazon.awssdk.services.sqs.SqsClientBuilder
import software.amazon.awssdk.services.sqs.model.SendMessageRequest

import java.net.URI
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.ZoneId
import scala.concurrent.duration.DurationInt

class SimpleQueueServiceStubTest extends CatsEffectSuite {

  private val ENDPOINT = URI.create("https://sqs.test")
  private val CREDENTIALS = StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret"))
  private val QUEUE_URL = "https://sqs.test/123456789012/queue"

  // SQS validates an MD5-of-body checksum on both send and receive, so stub responses must carry the real
  // MD5 of each message body or the SDK's MessageMD5ChecksumInterceptor rejects the response.
  private def md5(s: String): String =
    MessageDigest
      .getInstance("MD5")
      .digest(s.getBytes(StandardCharsets.UTF_8))
      .map(b => f"$b%02x")
      .mkString

  // SQS SDK v2 speaks the AWS JSON protocol.
  private def response(body: String): StubAwsHttpClient.Response =
    StubAwsHttpClient.Response(
      contentType = "application/x-amz-json-1.0",
      body = body.getBytes(StandardCharsets.UTF_8))

  private def configure_builder(
    transport: StubAwsHttpClient
  )(builder: SqsClientBuilder): SqsClientBuilder =
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

  private def service(
    transport: StubAwsHttpClient,
    policy: Policy.type => Policy = _ => Policy.empty): cats.effect.Resource[IO, SimpleQueueService[IO]] =
    SimpleQueueService[IO](ZoneId.of("UTC"), policy)(configure_builder(transport))

  test("1.receive produces SqsMessages with one-based batch/message indices and batch size") {
    // A single non-empty batch followed by the stream-terminating Policy.empty.
    val transport = StubAwsHttpClient.queue(
      Right(response(s"""{"Messages":[{"MessageId":"m1","ReceiptHandle":"r1","Body":"b1","MD5OfBody":"${md5(
          "b1")}"},{"MessageId":"m2","ReceiptHandle":"r2","Body":"b2","MD5OfBody":"${md5("b2")}"}]}""")),
      Right(response("""{"Messages":[]}"""))
    )

    service(transport).use { sqs =>
      sqs.receive(_.queueUrl(QUEUE_URL).maxNumberOfMessages(10)).compile.toList.map { messages =>
        assertEquals(messages.map(_.response.messageId()), List("m1", "m2"))
        assertEquals(messages.map(_.batchIndex), List(1L, 1L))
        assertEquals(messages.map(_.messageIndex), List(1, 2))
        assertEquals(messages.map(_.batchSize), List(2, 2))
      }
    }
  }

  test("2.receive terminates on an empty batch when the Policy exhausts") {
    val transport = StubAwsHttpClient.queue(Right(response("""{"Messages":[]}""")))

    service(transport).use { sqs =>
      sqs.receive(_.queueUrl(QUEUE_URL)).compile.toList.map(messages => assertEquals(messages, Nil))
    }
  }

  test("2b.receive re-polls after an empty batch while the Policy still has a tick") {
    // First poll is EMPTY, so with a one-tick fixed-delay policy the stream sleeps the tiny snooze and
    // re-polls; the second poll yields a message, proving the Some(ts) retry arm ran.
    val transport = StubAwsHttpClient.queue(
      Right(response("""{"Messages":[]}""")),
      Right(response(s"""{"Messages":[{"MessageId":"m1","ReceiptHandle":"r1","Body":"b1","MD5OfBody":"${md5(
          "b1")}"}]}""")),
      Right(response("""{"Messages":[]}"""))
    )

    service(transport, _ => Policy.fixedDelay(1.millis)).use { sqs =>
      sqs.receive(_.queueUrl(QUEUE_URL)).take(1).compile.toList.map { messages =>
        assertEquals(messages.map(_.response.messageId()), List("m1"))
      }
    }
  }

  test("3.delete issues a DeleteMessage using the message's queue url and receipt handle") {
    val transport = StubAwsHttpClient.queue(
      Right(
        response(s"""{"Messages":[{"MessageId":"m1","ReceiptHandle":"receipt-1","Body":"b","MD5OfBody":"${md5(
            "b")}"}]}""")),
      Right(response("""{"Messages":[]}""")),
      Right(response("""{}"""))
    )

    service(transport).use { sqs =>
      sqs.receive(_.queueUrl(QUEUE_URL)).compile.toList.flatMap { messages =>
        sqs.delete(messages.head).map { _ =>
          val last = transport.recordedRequests.last
          val body = io.circe.jawn.parse(last.bodyUtf8).fold(throw _, identity)
          assertEquals(body.hcursor.get[String]("QueueUrl").toOption, Some(QUEUE_URL))
          assertEquals(body.hcursor.get[String]("ReceiptHandle").toOption, Some("receipt-1"))
        }
      }
    }
  }

  test("4.resetVisibility issues a ChangeMessageVisibility with a zero timeout") {
    val transport = StubAwsHttpClient.queue(
      Right(
        response(s"""{"Messages":[{"MessageId":"m1","ReceiptHandle":"receipt-1","Body":"b","MD5OfBody":"${md5(
            "b")}"}]}""")),
      Right(response("""{"Messages":[]}""")),
      Right(response("""{}"""))
    )

    service(transport).use { sqs =>
      sqs.receive(_.queueUrl(QUEUE_URL)).compile.toList.flatMap { messages =>
        sqs.resetVisibility(messages.head).map { _ =>
          val last = transport.recordedRequests.last
          val body = io.circe.jawn.parse(last.bodyUtf8).fold(throw _, identity)
          assertEquals(body.hcursor.get[String]("QueueUrl").toOption, Some(QUEUE_URL))
          assertEquals(body.hcursor.get[String]("ReceiptHandle").toOption, Some("receipt-1"))
          assertEquals(body.hcursor.get[Int]("VisibilityTimeout").toOption, Some(0))
        }
      }
    }
  }

  test("5.send marshals the request body and decodes the response") {
    val transport = StubAwsHttpClient.queue(
      Right(response(s"""{"MessageId":"sent-1","MD5OfMessageBody":"${md5("payload")}"}""")))

    service(transport).use { sqs =>
      val request = SendMessageRequest.builder().queueUrl(QUEUE_URL).messageBody("payload").build()
      sqs.send(request).map { resp =>
        assertEquals(resp.messageId(), "sent-1")
        val body = io.circe.jawn.parse(transport.recordedRequests.head.bodyUtf8).fold(throw _, identity)
        assertEquals(body.hcursor.get[String]("QueueUrl").toOption, Some(QUEUE_URL))
        assertEquals(body.hcursor.get[String]("MessageBody").toOption, Some("payload"))
      }
    }
  }

  test("6.send Endo-builder overload marshals and decodes") {
    val transport = StubAwsHttpClient.queue(
      Right(response(s"""{"MessageId":"sent-2","MD5OfMessageBody":"${md5("hello")}"}""")))

    service(transport).use { sqs =>
      sqs.send(_.queueUrl(QUEUE_URL).messageBody("hello")).map { resp =>
        assertEquals(resp.messageId(), "sent-2")
        val body = io.circe.jawn.parse(transport.recordedRequests.head.bodyUtf8).fold(throw _, identity)
        assertEquals(body.hcursor.get[String]("MessageBody").toOption, Some("hello"))
      }
    }
  }

  test("7.transport failures propagate as SdkClientException wrapping the IOException") {
    val failure = new java.io.IOException("transport failed")
    val transport = StubAwsHttpClient.queue(Left(failure))

    service(transport).use { sqs =>
      interceptIO[software.amazon.awssdk.core.exception.SdkClientException](
        sqs.send(_.queueUrl(QUEUE_URL).messageBody("x"))).map { error =>
        assertEquals(error.getCause, failure)
      }
    }
  }

  test("8.managed resource release closes its owned transport exactly once") {
    val transport = StubAwsHttpClient.queue()

    service(transport).use(_ => IO.unit).map { _ =>
      assertEquals(transport.closeCalls, 1)
    }
  }

  test("9.sqsS3Parser parses an S3-event body with a URL-encoded key") {
    val body =
      """{"Records":[{"s3":{"bucket":{"name":"mybucket"},"object":{"key":"a%2Fb%20c.txt","size":7}}}]}"""
    val transport = StubAwsHttpClient.queue(
      Right(response(s"""{"Messages":[{"MessageId":"mid","ReceiptHandle":"r","Body":${io.circe.Json
          .fromString(body)
          .noSpaces},"MD5OfBody":"${md5(body)}"}]}""")),
      Right(response("""{"Messages":[]}"""))
    )

    service(transport).use { sqs =>
      sqs.receive(_.queueUrl(QUEUE_URL)).compile.toList.map { messages =>
        val files = sqsS3Parser(messages.head)
        assertEquals(files.map(_.path.bucket), List("mybucket"))
        assertEquals(files.map(_.path.key), List("a/b c.txt"))
        assertEquals(files.map(_.size), List(7L))
        assertEquals(files.map(_.messageId), List("mid"))
        assertEquals(files.map(_.queueUrl), List(QUEUE_URL))
      }
    }
  }

  test("10.sqsS3Parser returns Nil for a non-S3 body") {
    val transport = StubAwsHttpClient.queue(
      Right(
        response(
          s"""{"Messages":[{"MessageId":"mid","ReceiptHandle":"r","Body":"not json s3","MD5OfBody":"${md5(
              "not json s3")}"}]}""")),
      Right(response("""{"Messages":[]}"""))
    )

    service(transport).use { sqs =>
      sqs.receive(_.queueUrl(QUEUE_URL)).compile.toList.map { messages =>
        assertEquals(sqsS3Parser(messages.head), Nil)
      }
    }
  }
}
