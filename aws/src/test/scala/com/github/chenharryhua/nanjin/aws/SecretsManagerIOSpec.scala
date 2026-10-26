package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import io.circe.Json
import io.circe.jawn.parse
import munit.CatsEffectSuite
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.retries.StandardRetryStrategy
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClientBuilder
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest

import java.net.URI
import java.nio.charset.StandardCharsets

class SecretsManagerIOSpec extends CatsEffectSuite {

  private val ENDPOINT = URI.create("https://aws.test")
  private val CREDENTIALS = StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret"))

  private def response(body: String): StubAwsHttpClient.Response =
    StubAwsHttpClient.Response(body = body.getBytes(StandardCharsets.UTF_8))

  private def configure_builder(
    transport: StubAwsHttpClient
  )(builder: SecretsManagerClientBuilder): SecretsManagerClientBuilder =
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

  private def request_json(transport: StubAwsHttpClient): Json = {
    val requests = transport.recordedRequests
    assertEquals(requests.size, 1)
    parse(requests.head.bodyUtf8).fold(throw _, identity)
  }

  test("1.getValue sends SecretId and decodes the raw response") {
    val transport = StubAwsHttpClient.queue(Right(response(
      """{"ARN":"arn:aws:secretsmanager:us-east-1:123456789012:secret:raw","Name":"raw-secret","SecretString":"raw-value","VersionId":"version-1","VersionStages":["AWSCURRENT"]}""")))
    val request = GetSecretValueRequest.builder().secretId("raw-secret").build()

    SecretsManager[IO](configure_builder(transport)).use { manager =>
      manager.getValue(request).map { result =>
        assertEquals(result.name(), "raw-secret")
        assertEquals(result.secretString(), "raw-value")
        assertEquals(result.versionId(), "version-1")
        assertEquals(request_json(transport), Json.obj("SecretId" -> Json.fromString("raw-secret")))
      }
    }
  }

  test("2.getString returns SecretString") {
    val transport =
      StubAwsHttpClient.queue(Right(response("""{"Name":"string-secret","SecretString":"super-secret"}""")))

    SecretsManager[IO](configure_builder(transport)).use { manager =>
      manager.getString("string-secret").map { result =>
        assertEquals(result, "super-secret")
        assertEquals(request_json(transport), Json.obj("SecretId" -> Json.fromString("string-secret")))
      }
    }
  }

  test("3.getString rejects a binary-only response with its documented message") {
    val transport =
      StubAwsHttpClient.queue(Right(response("""{"Name":"binary-secret","SecretBinary":"Ynl0ZXM="}""")))

    SecretsManager[IO](configure_builder(transport)).use { manager =>
      interceptIO[IllegalStateException](manager.getString("binary-secret")).map { error =>
        assertEquals(
          error.getMessage,
          "Secret 'binary-secret' does not contain a string value. Use getBinary instead.")
      }
    }
  }

  test("4.getBinary decodes the service Base64 SecretBinary value") {
    val transport = StubAwsHttpClient.queue(
      Right(response("""{"Name":"binary-secret","SecretBinary":"YmluYXJ5LXNlY3JldA=="}""")))

    SecretsManager[IO](configure_builder(transport)).use { manager =>
      manager.getBinary("binary-secret").map { result =>
        assertEquals(result.asUtf8String(), "binary-secret")
        assertEquals(request_json(transport), Json.obj("SecretId" -> Json.fromString("binary-secret")))
      }
    }
  }

  test("5.getBinary rejects a string-only response with its documented message") {
    val transport =
      StubAwsHttpClient.queue(Right(response("""{"Name":"string-secret","SecretString":"text"}""")))

    SecretsManager[IO](configure_builder(transport)).use { manager =>
      interceptIO[IllegalStateException](manager.getBinary("string-secret")).map { error =>
        assertEquals(
          error.getMessage,
          "Secret 'string-secret' does not contain a binary value. Use getString instead.")
      }
    }
  }

  test("6.transport failures propagate from the managed adapter") {
    val failure = new IllegalStateException("transport failed")
    val transport = StubAwsHttpClient.queue(Left(failure))

    SecretsManager[IO](configure_builder(transport)).use { manager =>
      interceptIO[software.amazon.awssdk.core.exception.SdkClientException](
        manager.getString("failed-secret")).map { error =>
        assertEquals(error.getCause, failure)
      }
    }
  }

  test("7.managed resource release closes its owned transport exactly once") {
    val transport = StubAwsHttpClient.queue()

    SecretsManager[IO](configure_builder(transport)).use(_ => IO.unit).map { _ =>
      assertEquals(transport.closeCalls, 1)
    }
  }
}
