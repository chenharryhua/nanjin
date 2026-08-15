package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  GetObjectRequest,
  GetObjectResponse,
  HeadObjectRequest,
  HeadObjectResponse,
  PutObjectRequest,
  PutObjectResponse,
  CopyObjectRequest,
  CopyObjectResponse,
  DeleteObjectRequest,
  DeleteObjectResponse,
  RenameObjectRequest,
  RenameObjectResponse
}
import software.amazon.awssdk.services.s3.presigner.model.{GetObjectPresignRequest, PresignedGetObjectRequest}

import scala.concurrent.duration._

class SimpleStorageServiceIOSpec extends AnyFlatSpec with Matchers {

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
    }

  "SimpleStorageService" should "head object using request" in {
    val client = new FakeS3Client
    val service = mkService(client)

    val request = HeadObjectRequest.builder().bucket("bucket-a").key("key-a").build()
    val response = service.headObject(request).unsafeRunSync()

    response.eTag() shouldBe "fake-etag"
    client.lastHeadRequest.map(_.bucket()) shouldBe Some("bucket-a")
    client.lastHeadRequest.map(_.key()) shouldBe Some("key-a")
  }

  it should "head object using builder syntax" in {
    val client = new FakeS3Client
    val service = mkService(client)

    val response = service
      .headObject(_.bucket("bucket-b").key("key-b"))
      .unsafeRunSync()

    response.eTag() shouldBe "fake-etag"
    client.lastHeadRequest.map(_.bucket()) shouldBe Some("bucket-b")
    client.lastHeadRequest.map(_.key()) shouldBe Some("key-b")
  }

  it should "rename object using request" in {
    val client = new FakeS3Client
    val service = mkService(client)

    val request =
      RenameObjectRequest.builder().bucket("bucket-r").key("target").renameSource("source").build()
    service.renameObject(request).unsafeRunSync()

    client.lastRenameRequest.map(_.bucket()) shouldBe Some("bucket-r")
    client.lastRenameRequest.map(_.key()) shouldBe Some("target")
    client.lastRenameRequest.map(_.renameSource()) shouldBe Some("source")
  }

  it should "copy object using request" in {
    val client = new FakeS3Client
    val service = mkService(client)

    val request =
      CopyObjectRequest.builder()
        .sourceBucket("bucket-source")
        .sourceKey("key-source")
        .destinationBucket("bucket-target")
        .destinationKey("key-target")
        .build()
    service.copyObject(request).unsafeRunSync()

    client.lastCopyRequest.map(_.sourceBucket()) shouldBe Some("bucket-source")
    client.lastCopyRequest.map(_.sourceKey()) shouldBe Some("key-source")
    client.lastCopyRequest.map(_.destinationBucket()) shouldBe Some("bucket-target")
    client.lastCopyRequest.map(_.destinationKey()) shouldBe Some("key-target")
  }

  it should "copy object using builder syntax" in {
    val client = new FakeS3Client
    val service = mkService(client)

    service
      .copyObject(
        _.sourceBucket("bucket-source")
          .sourceKey("key-source")
          .destinationBucket("bucket-target")
          .destinationKey("key-target"))
      .unsafeRunSync()

    client.lastCopyRequest.map(_.sourceBucket()) shouldBe Some("bucket-source")
    client.lastCopyRequest.map(_.sourceKey()) shouldBe Some("key-source")
    client.lastCopyRequest.map(_.destinationBucket()) shouldBe Some("bucket-target")
    client.lastCopyRequest.map(_.destinationKey()) shouldBe Some("key-target")
  }

  it should "delete object using request and builder syntax" in {
    val client = new FakeS3Client
    val service = mkService(client)

    service.deleteObject(DeleteObjectRequest.builder().bucket("bucket-d").key("key-d").build()).unsafeRunSync()
    client.lastDeleteRequest.map(_.bucket()) shouldBe Some("bucket-d")
    client.lastDeleteRequest.map(_.key()) shouldBe Some("key-d")

    service.deleteObject(_.bucket("bucket-e").key("key-e")).unsafeRunSync()
    client.lastDeleteRequest.map(_.bucket()) shouldBe Some("bucket-e")
    client.lastDeleteRequest.map(_.key()) shouldBe Some("key-e")
  }

  it should "rename object using builder syntax" in {
    val client = new FakeS3Client
    val service = mkService(client)

    service
      .renameObject(_.bucket("bucket-s").key("dst").renameSource("src"))
      .unsafeRunSync()

    client.lastRenameRequest.map(_.bucket()) shouldBe Some("bucket-s")
    client.lastRenameRequest.map(_.key()) shouldBe Some("dst")
    client.lastRenameRequest.map(_.renameSource()) shouldBe Some("src")
  }

  it should "presign get object using request" in {
    val client = new FakeS3Client
    val service = mkService(client)

    val request =
      GetObjectPresignRequest
        .builder()
        .signatureDuration(java.time.Duration.ofMinutes(10))
        .getObjectRequest(GetObjectRequest.builder().bucket("bucket-p").key("key-p").build())
        .build()

    service.presignGetObject(request).unsafeRunSync()

    client.lastPresignRequest.map(_.getObjectRequest().bucket()) shouldBe Some("bucket-p")
    client.lastPresignRequest.map(_.getObjectRequest().key()) shouldBe Some("key-p")
  }

  it should "presign get object using builder syntax" in {
    val client = new FakeS3Client
    val service = mkService(client)

    service
      .presignGetObject(
        _.signatureDuration(java.time.Duration.ofMinutes(5))
          .getObjectRequest(GetObjectRequest.builder().bucket("bucket-b").key("key-b").build()))
      .unsafeRunSync()

    client.lastPresignRequest.map(_.getObjectRequest().bucket()) shouldBe Some("bucket-b")
    client.lastPresignRequest.map(_.getObjectRequest().key()) shouldBe Some("key-b")
  }

  it should "presign get object using an S3 URL" in {
    val client = new FakeS3Client
    val service = mkService(client)

    service.presignGetObject("s3://bucket-u/path/to/key-u", 5.minutes).unsafeRunSync()

    client.lastPresignRequest.map(_.getObjectRequest().bucket()) shouldBe Some("bucket-u")
    client.lastPresignRequest.map(_.getObjectRequest().key()) shouldBe Some("path/to/key-u")
    client.lastPresignRequest.map(_.signatureDuration()) shouldBe Some(java.time.Duration.ofMinutes(5))
  }

  it should "reject a non-S3 URL when presigning by URL" in {
    val service = mkService(new FakeS3Client)

    an[IllegalArgumentException] should be thrownBy
      service.presignGetObject("https://bucket.example.com/key", 5.minutes)
  }

  it should "reject an S3 URL without a bucket or key" in {
    val service = mkService(new FakeS3Client)

    an[IllegalArgumentException] should be thrownBy service.presignGetObject("s3:///key", 5.minutes)
    an[IllegalArgumentException] should be thrownBy service.presignGetObject("s3://bucket", 5.minutes)
  }

  it should "reject an invalid signature duration" in {
    val service = mkService(new FakeS3Client)

    an[IllegalArgumentException] should be thrownBy service.presignGetObject("s3://bucket/key", Duration.Zero)
    an[IllegalArgumentException] should be thrownBy service.presignGetObject("s3://bucket/key", -1.second)
  }

  it should "reject unsupported S3 URL components" in {
    val service = mkService(new FakeS3Client)

    an[IllegalArgumentException] should be thrownBy
      service.presignGetObject("s3://bucket/key?versionId=abc", 5.minutes)
    an[IllegalArgumentException] should be thrownBy
      service.presignGetObject("s3://user@bucket/key", 5.minutes)
  }
}
