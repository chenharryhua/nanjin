package com.github.chenharryhua.nanjin.aws

import software.amazon.awssdk.http.{
  AbortableInputStream,
  ExecutableHttpRequest,
  HttpExecuteRequest,
  HttpExecuteResponse,
  SdkHttpClient,
  SdkHttpResponse
}
import software.amazon.awssdk.utils.AttributeMap

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*
import scala.util.Using

final class StubAwsHttpClient private (
  response_function: StubAwsHttpClient.RecordedRequest => Either[Throwable, StubAwsHttpClient.Response])
    extends SdkHttpClient {

  private val recorded_requests = new ConcurrentLinkedQueue[StubAwsHttpClient.RecordedRequest]()
  private val close_count = new AtomicInteger()

  def recordedRequests: List[StubAwsHttpClient.RecordedRequest] =
    recorded_requests.iterator().asScala.toList

  def closeCalls: Int = close_count.get()

  def builder: StubAwsHttpClient.Builder = new StubAwsHttpClient.Builder(this)

  override def prepareRequest(request: HttpExecuteRequest): ExecutableHttpRequest = {
    val body = request.contentStreamProvider().toScala.fold(Array.emptyByteArray) { provider =>
      Using.resource(provider.newStream())(_.readAllBytes())
    }
    val recorded_request = StubAwsHttpClient.RecordedRequest(request, body)
    recorded_requests.add(recorded_request)

    new ExecutableHttpRequest {
      override def abort(): Unit = ()

      override def call(): HttpExecuteResponse =
        response_function(recorded_request) match {
          case Left(error)     => throw error
          case Right(response) => response.toHttpExecuteResponse
        }
    }
  }

  override def close(): Unit =
    close_count.incrementAndGet(): Unit
}

object StubAwsHttpClient {

  final case class RecordedRequest(request: HttpExecuteRequest, body: Array[Byte]) {
    def bodyUtf8: String = new String(body, StandardCharsets.UTF_8)
  }

  final case class Response(
    statusCode: Int = 200,
    contentType: String = "application/x-amz-json-1.1",
    headers: Map[String, List[String]] = Map.empty,
    body: Array[Byte] = Array.emptyByteArray) {

    private[aws] def toHttpExecuteResponse: HttpExecuteResponse = {
      val response_headers = (headers + ("Content-Type" -> List(contentType))).view
        .mapValues(_.asJava)
        .toMap
        .asJava
      val http_response = SdkHttpResponse.builder().statusCode(statusCode).headers(response_headers).build()
      HttpExecuteResponse
        .builder()
        .response(http_response)
        .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body.clone())))
        .build()
    }
  }

  final class Builder private[aws] (client: StubAwsHttpClient) extends SdkHttpClient.Builder[Builder] {
    override def buildWithDefaults(defaults: AttributeMap): SdkHttpClient = client
  }

  def apply(
    response: RecordedRequest => Either[Throwable, Response]
  ): StubAwsHttpClient = new StubAwsHttpClient(response)

  def queue(responses: Either[Throwable, Response]*): StubAwsHttpClient = {
    val response_queue = new ConcurrentLinkedQueue[Either[Throwable, Response]](responses.asJava)
    StubAwsHttpClient { _ =>
      Option(response_queue.poll()).getOrElse(
        Left(new NoSuchElementException("No stub AWS response remains")))
    }
  }
}
