package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import io.circe.Json
import io.circe.jawn.parse
import munit.CatsEffectSuite
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.retries.StandardRetryStrategy
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.ssm.SsmClientBuilder

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.jdk.CollectionConverters.*

class ParameterStoreTest extends CatsEffectSuite {

  private val ENDPOINT = URI.create("https://aws.test")
  private val CREDENTIALS = StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret"))

  private def response(body: String): StubAwsHttpClient.Response =
    StubAwsHttpClient.Response(body = body.getBytes(StandardCharsets.UTF_8))

  private def configure_builder(
    transport: StubAwsHttpClient
  )(builder: SsmClientBuilder): SsmClientBuilder =
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

  test("1.fetch(builder) marshals names and decryption and decodes the response") {
    val transport = StubAwsHttpClient.queue(
      Right(response("""{"Parameters":[{"Name":"first","Type":"String","Value":"one","Version":1}]}""")))

    ParameterStore[IO](configure_builder(transport)).use { store =>
      store.fetch(_.names("first", "second").withDecryption(false)).map { result =>
        val parameters = result.parameters().asScala.toList
        assertEquals(parameters.map(_.name()), List("first"))
        assertEquals(parameters.map(_.value()), List("one"))
        assertEquals(
          request_json(transport),
          Json.obj(
            "Names" -> Json.arr(Json.fromString("first"), Json.fromString("second")),
            "WithDecryption" -> Json.False))
      }
    }
  }

  test("2.fetch(path) defaults to decryption and returns the first parameter") {
    val transport = StubAwsHttpClient.queue(
      Right(response(
        """{"Parameters":[{"Name":"secure/path","Value":"first"},{"Name":"other","Value":"second"}]}""")))

    ParameterStore[IO](configure_builder(transport)).use { store =>
      store.fetch("secure/path").map { content =>
        assertEquals(content.value, "first")
        assertEquals(
          request_json(transport),
          Json.obj("Names" -> Json.arr(Json.fromString("secure/path")), "WithDecryption" -> Json.True))
      }
    }
  }

  test("3.fetch(path, isSecure = false) sends false") {
    val transport =
      StubAwsHttpClient.queue(Right(response("""{"Parameters":[{"Name":"plain/path","Value":"plain"}]}""")))

    ParameterStore[IO](configure_builder(transport)).use { store =>
      store.fetch("plain/path", isSecure = false).map { content =>
        assertEquals(content.value, "plain")
        assertEquals(
          request_json(transport),
          Json.obj("Names" -> Json.arr(Json.fromString("plain/path")), "WithDecryption" -> Json.False))
      }
    }
  }

  test("4.fetch(path) raises NoSuchElementException when the parameter is missing") {
    val transport = StubAwsHttpClient.queue(Right(response("""{"Parameters":[]}""")))

    ParameterStore[IO](configure_builder(transport)).use { store =>
      interceptIO[NoSuchElementException](store.fetch("missing/path")).map { error =>
        assertEquals(error.getMessage, "No parameter found at missing/path")
      }
    }
  }

  test("5.base64(path) uses secure default and decodes the returned content") {
    val encoded = Base64.getEncoder.encodeToString("hello".getBytes(StandardCharsets.UTF_8))
    val transport = StubAwsHttpClient.queue(
      Right(response(s"""{"Parameters":[{"Name":"encoded/path","Value":"$encoded"}]}""")))

    ParameterStore[IO](configure_builder(transport)).use { store =>
      store.base64("encoded/path").map { bytes =>
        assertEquals(new String(bytes, StandardCharsets.UTF_8), "hello")
        assertEquals(
          request_json(transport),
          Json.obj("Names" -> Json.arr(Json.fromString("encoded/path")), "WithDecryption" -> Json.True))
      }
    }
  }

  test("6.transport failures propagate from the managed adapter") {
    val failure = new IllegalStateException("transport failed")
    val transport = StubAwsHttpClient.queue(Left(failure))

    ParameterStore[IO](configure_builder(transport)).use { store =>
      interceptIO[IllegalStateException](store.fetch("failed/path")).map { error =>
        assertEquals(error, failure)
      }
    }
  }

  test("7.managed resource release closes its owned transport exactly once") {
    val transport = StubAwsHttpClient.queue()

    ParameterStore[IO](configure_builder(transport)).use(_ => IO.unit).map { _ =>
      assertEquals(transport.closeCalls, 1)
    }
  }
}
