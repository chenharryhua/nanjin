package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import munit.CatsEffectSuite
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.retries.StandardRetryStrategy
import software.amazon.awssdk.services.s3.S3ClientBuilder

import java.net.URI
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.DurationInt

class SimpleStorageServiceStubTest extends CatsEffectSuite {

  private val ENDPOINT = URI.create("https://s3.test")
  private val CREDENTIALS = StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret"))

  // S3 uses the REST-XML protocol.
  private def xml(body: String, headers: Map[String, List[String]] = Map.empty): StubAwsHttpClient.Response =
    StubAwsHttpClient.Response(
      contentType = "application/xml",
      headers = headers,
      body = body.getBytes(StandardCharsets.UTF_8))

  private def configure_builder(
    transport: StubAwsHttpClient
  )(builder: S3ClientBuilder): S3ClientBuilder =
    builder
      .httpClientBuilder(transport.builder)
      .credentialsProvider(CREDENTIALS)
      .region(Region.US_EAST_1)
      .endpointOverride(ENDPOINT)
      .forcePathStyle(true)
      .overrideConfiguration(
        ClientOverrideConfiguration
          .builder()
          .retryStrategy(StandardRetryStrategy.builder().maxAttempts(1).build())
          .build())

  private def service(transport: StubAwsHttpClient): cats.effect.Resource[IO, SimpleStorageService[IO]] =
    SimpleStorageService[IO](configure_builder(transport))

  test("1.putObject uploads the body and decodes the ETag header") {
    val transport = StubAwsHttpClient.queue(Right(xml("", headers = Map("ETag" -> List("\"etag-1\"")))))

    service(transport).use { s3 =>
      s3.putObject(RequestBody.fromString("hello"), _.bucket("bucket").key("key")).map { resp =>
        assertEquals(resp.eTag(), "\"etag-1\"")
        val recorded = transport.recordedRequests.head
        // S3 uploads the payload with aws-chunked encoding plus a trailing checksum, so the raw wire body
        // wraps "hello" in chunk framing rather than equalling it exactly.
        assert(recorded.bodyUtf8.contains("hello"), recorded.bodyUtf8)
        assert(recorded.request.httpRequest().encodedPath().contains("key"))
      }
    }
  }

  test("2.getObject streams the object bytes and closes the stream on release") {
    val transport = StubAwsHttpClient.queue(Right(xml("payload-bytes")))

    service(transport).use { s3 =>
      s3.getObject(_.bucket("bucket").key("key")).use { stream =>
        IO(new String(stream.readAllBytes(), StandardCharsets.UTF_8))
      }.map(content => assertEquals(content, "payload-bytes"))
    }
  }

  test("3.headObject decodes metadata headers") {
    val transport = StubAwsHttpClient.queue(
      Right(xml("", headers = Map("ETag" -> List("\"head-etag\""), "Content-Length" -> List("42")))))

    service(transport).use { s3 =>
      s3.headObject(_.bucket("bucket").key("key")).map { resp =>
        assertEquals(resp.eTag(), "\"head-etag\"")
        assertEquals(resp.contentLength().longValue(), 42L)
      }
    }
  }

  test("4.deleteObject issues a delete and decodes the response") {
    val transport = StubAwsHttpClient.queue(Right(xml("").copy(statusCode = 204)))

    service(transport).use { s3 =>
      s3.deleteObject(_.bucket("bucket").key("key")).map { resp =>
        assert(resp != null)
        assertEquals(transport.recordedRequests.head.request.httpRequest().method().name(), "DELETE")
      }
    }
  }

  test("5.copyObject marshals source and decodes the XML result") {
    val copyXml =
      """<CopyObjectResult><ETag>"copy-etag"</ETag><LastModified>2021-01-01T00:00:00.000Z</LastModified></CopyObjectResult>"""
    val transport = StubAwsHttpClient.queue(Right(xml(copyXml)))

    service(transport).use { s3 =>
      s3.copyObject(
        _.sourceBucket("src").sourceKey("src-key").destinationBucket("dst").destinationKey("dst-key")).map {
        resp =>
          assertEquals(resp.copyObjectResult().eTag(), "\"copy-etag\"")
      }
    }
  }

  test("6.getObject failure propagates the S3 error document as an SDK exception") {
    val errorXml =
      """<Error><Code>NoSuchKey</Code><Message>The resource you requested does not exist</Message></Error>"""
    val transport = StubAwsHttpClient.queue(Right(xml(errorXml).copy(statusCode = 404)))

    service(transport).use { s3 =>
      interceptIO[software.amazon.awssdk.services.s3.model.S3Exception](
        s3.getObject(_.bucket("bucket").key("missing")).use_).map { error =>
        assertEquals(error.statusCode(), 404)
      }
    }
  }

  test("7.transport failures propagate as SdkClientException wrapping the IOException") {
    val failure = new java.io.IOException("transport failed")
    val transport = StubAwsHttpClient.queue(Left(failure))

    service(transport).use { s3 =>
      interceptIO[software.amazon.awssdk.core.exception.SdkClientException](
        s3.deleteObject(_.bucket("bucket").key("key"))).map { error =>
        assertEquals(error.getCause, failure)
      }
    }
  }

  test("8.presignGetObject from an S3 url builds a signed request without calling the transport") {
    val transport = StubAwsHttpClient.queue()

    service(transport).use { s3 =>
      s3.presignGetObject("s3://bucket/path/to/key", 10.minutes).map { presigned =>
        assertEquals(presigned.httpRequest().method().name(), "GET")
        assert(presigned.url().toString.contains("X-Amz-Signature"), presigned.url().toString)
        // presign is local-only; the HTTP transport is never invoked
        assertEquals(transport.recordedRequests, Nil)
      }
    }
  }

  test("9.managed resource release closes its owned transport exactly once") {
    val transport = StubAwsHttpClient.queue()

    service(transport).use(_ => IO.unit).map { _ =>
      assertEquals(transport.closeCalls, 1)
    }
  }
}
