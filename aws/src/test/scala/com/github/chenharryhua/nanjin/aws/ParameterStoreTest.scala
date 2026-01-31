package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.aws.{ParameterStoreContent, ParameterStorePath}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.ssm.SsmClient
import software.amazon.awssdk.services.ssm.model.{GetParametersRequest, GetParametersResponse, Parameter}

import scala.jdk.CollectionConverters.*

class ParameterStoreTest extends AnyFunSuite {

  /** Mock SSM client for testing */
  private class MockSsmClient(parameters: Map[String, String]) extends SsmClient {
    override def getParameters(request: GetParametersRequest): GetParametersResponse = {
      val params = request
        .names()
        .asScala
        .flatMap { name =>
          parameters.get(name).map(v => Parameter.builder().name(name).value(v).build())
        }
        .toList
        .asJava
      GetParametersResponse.builder().parameters(params).build()
    }

    override def close(): Unit = ()

    override def serviceName(): String = "abc"
  }

  /** Wrap a ParameterStore with a mock client */
  private def createStore(params: Map[String, String]): IO[ParameterStore[IO]] =
    for {
      _ <- Slf4jLogger.create[IO]
    } yield new ParameterStore[IO] {
      private val client = new MockSsmClient(params)

      override def fetch(request: GetParametersRequest): IO[GetParametersResponse] =
        IO.blocking(client.getParameters(request))

      override def fetch(path: ParameterStorePath): IO[ParameterStoreContent] =
        IO.blocking {
          client
            .getParameters(
              GetParametersRequest.builder().names(path.value).withDecryption(path.isSecure).build())
            .parameters()
            .asScala
            .headOption match {
            case Some(p) => ParameterStoreContent(p.value())
            case None    => throw new NoSuchElementException(s"No parameter found at ${path.value}")
          }
        }
    }

  test("fetch returns the correct parameter") {
    val store = createStore(Map("foo" -> "bar")).unsafeRunSync()
    val path = ParameterStorePath("foo", isSecure = false)

    val result = store.fetch(path).unsafeRunSync()
    assert(result.value == "bar")
  }

  test("fetch throws NoSuchElementException for missing parameter") {
    val store = createStore(Map("foo" -> "bar")).unsafeRunSync()
    val path = ParameterStorePath("missing", isSecure = false)

    assertThrows[NoSuchElementException](store.fetch(path).unsafeRunSync())
  }

  test("base64 decodes parameter value") {
    val encoded = java.util.Base64.getEncoder.encodeToString("hello".getBytes)
    val store = createStore(Map("baz" -> encoded)).unsafeRunSync()
    val path = ParameterStorePath("baz", isSecure = false)

    val result = store.base64(path).unsafeRunSync()
    assert(new String(result) == "hello")
  }

  test("fetch with GetParametersRequest returns correct value") {
    val store = createStore(Map("key" -> "value")).unsafeRunSync()
    val request = GetParametersRequest.builder().names("key").build()
    val result = store.fetch(request).unsafeRunSync()

    assert(result.parameters().asScala.head.value() == "value")
  }
}
