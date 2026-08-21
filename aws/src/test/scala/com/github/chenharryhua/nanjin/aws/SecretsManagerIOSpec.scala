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

  /** Simulates SecretsManagerImpl null-check behavior by returning responses with only one field set */
  final class NullAwareSecretsManager extends SecretsManager[IO] {

    override def getValue(req: GetSecretValueRequest): IO[GetSecretValueResponse] =
      IO.pure {
        req.secretId() match {
          case "only-string" =>
            GetSecretValueResponse.builder().secretString("hello").build()
          case "only-binary" =>
            GetSecretValueResponse.builder().secretBinary(SdkBytes.fromUtf8String("bytes")).build()
          case other =>
            throw new NoSuchElementException(s"Unknown secret: $other")
        }
      }

    override def getString(secretId: String): IO[String] =
      getValue(GetSecretValueRequest.builder().secretId(secretId).build()).flatMap { resp =>
        Option(resp.secretString()) match {
          case Some(s) => IO.pure(s)
          case None    =>
            IO.raiseError(
              new IllegalStateException(
                s"Secret '$secretId' does not contain a string value. Use getBinary instead."))
        }
      }

    override def getBinary(secretId: String): IO[SdkBytes] =
      getValue(GetSecretValueRequest.builder().secretId(secretId).build()).flatMap { resp =>
        Option(resp.secretBinary()) match {
          case Some(b) => IO.pure(b)
          case None    =>
            IO.raiseError(
              new IllegalStateException(
                s"Secret '$secretId' does not contain a binary value. Use getString instead."))
        }
      }
  }

  "NullAwareSecretsManager" should "return string when secret is a string secret" in {
    val sm = new NullAwareSecretsManager
    sm.getString("only-string").unsafeRunSync() shouldBe "hello"
  }

  it should "return binary when secret is a binary secret" in {
    val sm = new NullAwareSecretsManager
    sm.getBinary("only-binary").unsafeRunSync().asUtf8String() shouldBe "bytes"
  }

  it should "fail with IllegalStateException when getString called on binary secret" in {
    val sm = new NullAwareSecretsManager
    val ex = intercept[IllegalStateException] {
      sm.getString("only-binary").unsafeRunSync()
    }
    ex.getMessage should include("does not contain a string value")
    ex.getMessage should include("only-binary")
  }

  it should "fail with IllegalStateException when getBinary called on string secret" in {
    val sm = new NullAwareSecretsManager
    val ex = intercept[IllegalStateException] {
      sm.getBinary("only-string").unsafeRunSync()
    }
    ex.getMessage should include("does not contain a binary value")
    ex.getMessage should include("only-string")
  }
}
