package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.secretsmanager.model.*

class SecretsManagerIOSpec extends AnyFlatSpec with Matchers {

  /** Pure in-memory implementation for testing */
  final class DummySecretsManager extends SecretsManager[IO] {
    private val map = Map(
      "string-secret" -> ("super-secret", SdkBytes.fromUtf8String("binary-data")),
      "binary-secret" -> ("ignored-str", SdkBytes.fromUtf8String("binary-secret"))
    )

    override def getValue(req: GetSecretValueRequest): IO[GetSecretValueResponse] =
      IO.fromEither(
        map
          .get(req.secretId())
          .toRight(new NoSuchElementException(s"Secret not found: ${req.secretId()}"))
          .map { case (str, bin) =>
            GetSecretValueResponse.builder().secretString(str).secretBinary(bin).build()
          }
      )

    override def getString(secretId: String): IO[String] =
      getValue(GetSecretValueRequest.builder().secretId(secretId).build()).map(_.secretString())

    override def getBinary(secretId: String): IO[SdkBytes] =
      getValue(GetSecretValueRequest.builder().secretId(secretId).build()).map(_.secretBinary())
  }

  "DummySecretsManager" should "return secret string" in {
    val sm = new DummySecretsManager
    val result = sm.getString("string-secret").unsafeRunSync()
    result shouldBe "super-secret"
  }

  it should "return secret binary" in {
    val sm = new DummySecretsManager
    val result = sm.getBinary("binary-secret").unsafeRunSync()
    result.asUtf8String() shouldBe "binary-secret"
  }

  it should "fail for unknown secret" in {
    val sm = new DummySecretsManager
    val ex = intercept[NoSuchElementException] {
      sm.getString("missing-secret").unsafeRunSync()
    }
    ex.getMessage should include("Secret not found")
  }
}
