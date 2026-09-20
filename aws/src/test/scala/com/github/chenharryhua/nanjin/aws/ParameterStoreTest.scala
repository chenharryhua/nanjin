package com.github.chenharryhua.nanjin.aws

import cats.effect.IO
import munit.CatsEffectSuite
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.ssm.SsmClient
import software.amazon.awssdk.services.ssm.model.{GetParametersRequest, GetParametersResponse, Parameter}

import java.util.Base64
import scala.jdk.CollectionConverters.*

class ParameterStoreTest extends CatsEffectSuite {

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

      override def fetch(path: String, isSecure: Boolean): IO[ParameterStoreContent] =
        IO.blocking {
          client
            .getParameters(GetParametersRequest.builder().names(path).withDecryption(isSecure).build())
            .parameters()
            .asScala
            .headOption match {
            case Some(p) => ParameterStoreContent(p.value())
            case None    => throw new NoSuchElementException(s"No parameter found at $path")
          }
        }
      override def base64(path: String, isSecure: Boolean): IO[Array[Byte]] =
        fetch(path, isSecure).map(c => Base64.getDecoder.decode(c.value.getBytes))
    }

  test("1.fetch returns the correct parameter") {
    createStore(Map("foo" -> "bar")).flatMap { store =>
      store.fetch("foo", isSecure = false).map { result =>
        assert(result.value == "bar")
      }
    }
  }

  test("2.fetch throws NoSuchElementException for missing parameter") {
    createStore(Map("foo" -> "bar")).flatMap { store =>
      interceptIO[NoSuchElementException](store.fetch("missing", isSecure = false))
    }
  }

  test("3.base64 decodes parameter value") {
    val encoded = java.util.Base64.getEncoder.encodeToString("hello".getBytes)
    createStore(Map("baz" -> encoded)).flatMap { store =>
      store.base64("baz", isSecure = false).map { result =>
        assert(new String(result) == "hello")
      }
    }
  }

  test("4.fetch with GetParametersRequest returns correct value") {
    createStore(Map("key" -> "value")).flatMap { store =>
      val request = GetParametersRequest.builder().names("key").build()
      store.fetch(request).map { result =>
        assert(result.parameters().asScala.head.value() == "value")
      }
    }
  }
}
