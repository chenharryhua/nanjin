package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import munit.CatsEffectSuite
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.secretsmanager.model.*

class SecretsManagerIOSpec extends CatsEffectSuite {

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

  test("DummySecretsManager: return secret string") {
    val sm = new DummySecretsManager
    sm.getString("string-secret").map { result =>
      assertEquals(result, "super-secret")
    }
  }

  test("DummySecretsManager: return secret binary") {
    val sm = new DummySecretsManager
    sm.getBinary("binary-secret").map { result =>
      assertEquals(result.asUtf8String(), "binary-secret")
    }
  }

  test("DummySecretsManager: fail for unknown secret") {
    val sm = new DummySecretsManager
    interceptIO[NoSuchElementException] {
      sm.getString("missing-secret")
    }.map { ex =>
      assert(ex.getMessage.contains("Secret not found"))
    }
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

  test("NullAwareSecretsManager: return string when secret is a string secret") {
    val sm = new NullAwareSecretsManager
    sm.getString("only-string").map { result =>
      assertEquals(result, "hello")
    }
  }

  test("NullAwareSecretsManager: return binary when secret is a binary secret") {
    val sm = new NullAwareSecretsManager
    sm.getBinary("only-binary").map { result =>
      assertEquals(result.asUtf8String(), "bytes")
    }
  }

  test("NullAwareSecretsManager: fail with IllegalStateException when getString called on binary secret") {
    val sm = new NullAwareSecretsManager
    interceptIO[IllegalStateException] {
      sm.getString("only-binary")
    }.map { ex =>
      assert(ex.getMessage.contains("does not contain a string value"))
      assert(ex.getMessage.contains("only-binary"))
    }
  }

  test("NullAwareSecretsManager: fail with IllegalStateException when getBinary called on string secret") {
    val sm = new NullAwareSecretsManager
    interceptIO[IllegalStateException] {
      sm.getBinary("only-string")
    }.map { ex =>
      assert(ex.getMessage.contains("does not contain a binary value"))
      assert(ex.getMessage.contains("only-string"))
    }
  }
}
