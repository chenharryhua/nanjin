package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  HeadObjectRequest,
  HeadObjectResponse,
  RenameObjectRequest,
  RenameObjectResponse
}

class SimpleStorageServiceIOSpec extends AnyFlatSpec with Matchers {

  final private class FakeS3Client extends S3Client {
    @volatile var lastHeadRequest: Option[HeadObjectRequest] = None
    @volatile var lastRenameRequest: Option[RenameObjectRequest] = None

    override def headObject(request: HeadObjectRequest): HeadObjectResponse = {
      lastHeadRequest = Some(request)
      HeadObjectResponse.builder().eTag("fake-etag").build()
    }

    override def renameObject(request: RenameObjectRequest): RenameObjectResponse = {
      lastRenameRequest = Some(request)
      RenameObjectResponse.builder().build()
    }

    override def close(): Unit = ()

    override def serviceName(): String = "abc"
  }

  private def mkService(client: FakeS3Client): SimpleStorageService[IO] =
    new SimpleStorageService[IO] {
      override def headObject(hor: HeadObjectRequest): IO[HeadObjectResponse] =
        IO(client.headObject(hor))

      override def renameObject(ror: RenameObjectRequest): IO[RenameObjectResponse] =
        IO(client.renameObject(ror))
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
}
