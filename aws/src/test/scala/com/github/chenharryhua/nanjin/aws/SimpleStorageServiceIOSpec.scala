package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import cats.effect.kernel.Resource
import munit.CatsEffectSuite
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  CopyObjectRequest,
  CopyObjectResponse,
  DeleteObjectRequest,
  DeleteObjectResponse,
  GetObjectRequest,
  GetObjectResponse,
  HeadObjectRequest,
  HeadObjectResponse,
  PutObjectRequest,
  PutObjectResponse,
  RenameObjectRequest,
  RenameObjectResponse
}
import software.amazon.awssdk.services.s3.presigner.model.{GetObjectPresignRequest, PresignedGetObjectRequest}

import java.net.URI
import scala.concurrent.duration._
import scala.jdk.DurationConverters.ScalaDurationOps

class SimpleStorageServiceIOSpec extends CatsEffectSuite {

  final private class FakeS3Client extends S3Client {
    @volatile var lastHeadRequest: Option[HeadObjectRequest] = None
    @volatile var lastGetRequest: Option[GetObjectRequest] = None
    @volatile var lastPutRequest: Option[PutObjectRequest] = None
    @volatile var lastPutBody: Option[RequestBody] = None
    @volatile var lastCopyRequest: Option[CopyObjectRequest] = None
    @volatile var lastDeleteRequest: Option[DeleteObjectRequest] = None
    @volatile var lastRenameRequest: Option[RenameObjectRequest] = None
    @volatile var lastPresignRequest: Option[GetObjectPresignRequest] = None

    override def headObject(request: HeadObjectRequest): HeadObjectResponse = {
      lastHeadRequest = Some(request)
      HeadObjectResponse.builder().eTag("fake-etag").build()
    }

    override def renameObject(request: RenameObjectRequest): RenameObjectResponse = {
      lastRenameRequest = Some(request)
      RenameObjectResponse.builder().build()
    }

    override def copyObject(request: CopyObjectRequest): CopyObjectResponse = {
      lastCopyRequest = Some(request)
      CopyObjectResponse.builder().build()
    }

    override def deleteObject(request: DeleteObjectRequest): DeleteObjectResponse = {
      lastDeleteRequest = Some(request)
      DeleteObjectResponse.builder().build()
    }

    override def close(): Unit = ()

    override def serviceName(): String = "abc"
  }

  private def mkService(client: FakeS3Client): SimpleStorageService[IO] =
    new SimpleStorageService[IO] {
      override def headObject(hor: HeadObjectRequest): IO[HeadObjectResponse] =
        IO(client.headObject(hor))

      override def getObject(gor: GetObjectRequest): Resource[IO, ResponseInputStream[GetObjectResponse]] =
        Resource.make {
          IO {
            client.lastGetRequest = Some(gor)
            null.asInstanceOf[ResponseInputStream[GetObjectResponse]]
          }
        }(_ => IO.unit)

      override def putObject(body: RequestBody, por: PutObjectRequest): IO[PutObjectResponse] =
        IO {
          client.lastPutRequest = Some(por)
          client.lastPutBody = Some(body)
          null.asInstanceOf[PutObjectResponse]
        }

      override def copyObject(cor: CopyObjectRequest): IO[CopyObjectResponse] =
        IO(client.copyObject(cor))

      override def deleteObject(cor: DeleteObjectRequest): IO[DeleteObjectResponse] =
        IO(client.deleteObject(cor))

      override def renameObject(ror: RenameObjectRequest): IO[RenameObjectResponse] =
        IO(client.renameObject(ror))

      override def presignGetObject(gpr: GetObjectPresignRequest): IO[PresignedGetObjectRequest] =
        IO {
          client.lastPresignRequest = Some(gpr)
          null.asInstanceOf[PresignedGetObjectRequest]
        }

      override def presignGetObject(
        s3Url: String,
        duration: FiniteDuration): IO[PresignedGetObjectRequest] = {
        val uri = URI(s3Url)
        val bucket = Option(uri.getHost).getOrElse("")
        val key = uri.getPath.stripPrefix("/")
        presignGetObject(
          _.signatureDuration(duration.toJava)
            .getObjectRequest(_.bucket(bucket).key(key): Unit))
      }
    }

  test("SimpleStorageService: head object using request") {
    val client = new FakeS3Client
    val service = mkService(client)

    val request = HeadObjectRequest.builder().bucket("bucket-a").key("key-a").build()
    service.headObject(request).map { response =>
      assertEquals(response.eTag(), "fake-etag")
      assertEquals(client.lastHeadRequest.map(_.bucket()), Some("bucket-a"))
      assertEquals(client.lastHeadRequest.map(_.key()), Some("key-a"))
    }
  }

  test("SimpleStorageService: head object using builder syntax") {
    val client = new FakeS3Client
    val service = mkService(client)

    service
      .headObject(_.bucket("bucket-b").key("key-b"))
      .map { response =>
        assertEquals(response.eTag(), "fake-etag")
        assertEquals(client.lastHeadRequest.map(_.bucket()), Some("bucket-b"))
        assertEquals(client.lastHeadRequest.map(_.key()), Some("key-b"))
      }
  }

  test("SimpleStorageService: rename object using request") {
    val client = new FakeS3Client
    val service = mkService(client)

    val request =
      RenameObjectRequest.builder().bucket("bucket-r").key("target").renameSource("source").build()
    service.renameObject(request).map { _ =>
      assertEquals(client.lastRenameRequest.map(_.bucket()), Some("bucket-r"))
      assertEquals(client.lastRenameRequest.map(_.key()), Some("target"))
      assertEquals(client.lastRenameRequest.map(_.renameSource()), Some("source"))
    }
  }

  test("SimpleStorageService: copy object using request") {
    val client = new FakeS3Client
    val service = mkService(client)

    val request =
      CopyObjectRequest.builder()
        .sourceBucket("bucket-source")
        .sourceKey("key-source")
        .destinationBucket("bucket-target")
        .destinationKey("key-target")
        .build()
    service.copyObject(request).map { _ =>
      assertEquals(client.lastCopyRequest.map(_.sourceBucket()), Some("bucket-source"))
      assertEquals(client.lastCopyRequest.map(_.sourceKey()), Some("key-source"))
      assertEquals(client.lastCopyRequest.map(_.destinationBucket()), Some("bucket-target"))
      assertEquals(client.lastCopyRequest.map(_.destinationKey()), Some("key-target"))
    }
  }

  test("SimpleStorageService: copy object using builder syntax") {
    val client = new FakeS3Client
    val service = mkService(client)

    service
      .copyObject(
        _.sourceBucket("bucket-source")
          .sourceKey("key-source")
          .destinationBucket("bucket-target")
          .destinationKey("key-target"))
      .map { _ =>
        assertEquals(client.lastCopyRequest.map(_.sourceBucket()), Some("bucket-source"))
        assertEquals(client.lastCopyRequest.map(_.sourceKey()), Some("key-source"))
        assertEquals(client.lastCopyRequest.map(_.destinationBucket()), Some("bucket-target"))
        assertEquals(client.lastCopyRequest.map(_.destinationKey()), Some("key-target"))
      }
  }

  test("SimpleStorageService: delete object using request and builder syntax") {
    val client = new FakeS3Client
    val service = mkService(client)

    service
      .deleteObject(DeleteObjectRequest.builder().bucket("bucket-d").key("key-d").build())
      .flatMap { _ =>
        assertEquals(client.lastDeleteRequest.map(_.bucket()), Some("bucket-d"))
        assertEquals(client.lastDeleteRequest.map(_.key()), Some("key-d"))

        service.deleteObject(_.bucket("bucket-e").key("key-e")).map { _ =>
          assertEquals(client.lastDeleteRequest.map(_.bucket()), Some("bucket-e"))
          assertEquals(client.lastDeleteRequest.map(_.key()), Some("key-e"))
        }
      }
  }

  test("SimpleStorageService: rename object using builder syntax") {
    val client = new FakeS3Client
    val service = mkService(client)

    service
      .renameObject(_.bucket("bucket-s").key("dst").renameSource("src"))
      .map { _ =>
        assertEquals(client.lastRenameRequest.map(_.bucket()), Some("bucket-s"))
        assertEquals(client.lastRenameRequest.map(_.key()), Some("dst"))
        assertEquals(client.lastRenameRequest.map(_.renameSource()), Some("src"))
      }
  }

  test("SimpleStorageService: presign get object using request") {
    val client = new FakeS3Client
    val service = mkService(client)

    val request =
      GetObjectPresignRequest
        .builder()
        .signatureDuration(java.time.Duration.ofMinutes(10))
        .getObjectRequest(GetObjectRequest.builder().bucket("bucket-p").key("key-p").build())
        .build()

    service.presignGetObject(request).map { _ =>
      assertEquals(client.lastPresignRequest.map(_.getObjectRequest().bucket()), Some("bucket-p"))
      assertEquals(client.lastPresignRequest.map(_.getObjectRequest().key()), Some("key-p"))
    }
  }

  test("SimpleStorageService: presign get object using builder syntax") {
    val client = new FakeS3Client
    val service = mkService(client)

    service
      .presignGetObject(
        _.signatureDuration(java.time.Duration.ofMinutes(5))
          .getObjectRequest(GetObjectRequest.builder().bucket("bucket-b").key("key-b").build()))
      .map { _ =>
        assertEquals(client.lastPresignRequest.map(_.getObjectRequest().bucket()), Some("bucket-b"))
        assertEquals(client.lastPresignRequest.map(_.getObjectRequest().key()), Some("key-b"))
      }
  }

  test("SimpleStorageService: presign get object using an S3 URL") {
    val client = new FakeS3Client
    val service = mkService(client)

    service.presignGetObject("s3://bucket-u/path/to/key-u", 5.minutes).map { _ =>
      assertEquals(client.lastPresignRequest.map(_.getObjectRequest().bucket()), Some("bucket-u"))
      assertEquals(client.lastPresignRequest.map(_.getObjectRequest().key()), Some("path/to/key-u"))
      assertEquals(
        client.lastPresignRequest.map(_.signatureDuration()),
        Some(java.time.Duration.ofMinutes(5)))
    }
  }

  test("SimpleStorageService: accept a URI with a host and path when presigning by URL") {
    val client = new FakeS3Client
    val service = mkService(client)

    service.presignGetObject("https://bucket.example.com/key", 5.minutes).map { _ =>
      assertEquals(client.lastPresignRequest.map(_.getObjectRequest().bucket()), Some("bucket.example.com"))
      assertEquals(client.lastPresignRequest.map(_.getObjectRequest().key()), Some("key"))
    }
  }

  test("SimpleStorageService: delegate URI validation to S3") {
    val client = new FakeS3Client
    val service = mkService(client)

    service.presignGetObject("s3://user@bucket/key?versionId=abc", 5.minutes).map { _ =>
      assertEquals(client.lastPresignRequest.map(_.getObjectRequest().bucket()), Some("bucket"))
      assertEquals(client.lastPresignRequest.map(_.getObjectRequest().key()), Some("key"))
    }
  }
}
