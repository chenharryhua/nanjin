package com.github.chenharryhua.nanjin.aws

import cats.Id
import fs2.Stream
import munit.FunSuite
import software.amazon.awssdk.services.sqs.model.*

/** Tests for `SimpleQueueService` that do not require a live SQS endpoint:
  *
  *   - `SqsMessage.asJson`/`toString`: the message renders only its batch coordinates and never leaks the raw
  *     request or message body.
  *   - the trait's `final` builder-overloads of `receive`/`send`: they apply the builder function and
  *     delegate to the request-taking method. A capturing stub of the trait records what it was handed.
  */
class SimpleQueueServiceTest extends FunSuite {

  private def message(batchIndex: Long, messageIndex: Int, batchSize: Int): SqsMessage =
    SqsMessage(
      request = ReceiveMessageRequest.builder().queueUrl("q").build(),
      response = Message.builder().body("TOP-SECRET-BODY").messageId("m-1").build(),
      batchIndex = batchIndex,
      messageIndex = messageIndex,
      batchSize = batchSize
    )

  // ---- SqsMessage rendering ------------------------------------------------------------------------

  test("1.asJson renders exactly the batch coordinates") {
    val json = message(batchIndex = 7L, messageIndex = 2, batchSize = 5).asJson
    val c = json.hcursor
    assert(c.get[Long]("batchIndex").toOption.contains(7L))
    assert(c.get[Int]("messageIndex").toOption.contains(2))
    assert(c.get[Int]("batchSize").toOption.contains(5))
    // no other keys
    assert(json.asObject.map(_.keys.toSet).contains(Set("batchIndex", "messageIndex", "batchSize")))
  }

  test("2.asJson never leaks the request or the raw message body") {
    val rendered = message(batchIndex = 1L, messageIndex = 1, batchSize = 1).asJson.noSpaces
    // the body is the user's data and must not appear in the metadata render
    assert(!rendered.contains("TOP-SECRET-BODY"))
    assert(!rendered.contains("m-1")) // messageId not rendered either
    assert(!rendered.toLowerCase.contains("body"))
  }

  test("3.toString equals asJson.noSpaces") {
    val msg = message(batchIndex = 3L, messageIndex = 4, batchSize = 9)
    assert(msg.toString == msg.asJson.noSpaces)
  }

  test("4.asJson tolerates null request/response (only coordinates are read)") {
    val msg = SqsMessage(request = null, response = null, batchIndex = 0L, messageIndex = 0, batchSize = 0)
    val c = msg.asJson.hcursor
    assert(c.get[Long]("batchIndex").toOption.contains(0L))
    assert(c.get[Int]("messageIndex").toOption.contains(0))
    assert(c.get[Int]("batchSize").toOption.contains(0))
  }

  // ---- trait convenience overloads -----------------------------------------------------------------

  /** Captures the request handed to the abstract methods so the `final` builder-overloads can be checked in
    * isolation, without a live client.
    */
  final private class CapturingSqs extends SimpleQueueService[Id] {
    var lastReceive: Option[ReceiveMessageRequest] = None
    var lastSend: Option[SendMessageRequest] = None

    override def receive(request: ReceiveMessageRequest): Stream[Id, SqsMessage] = {
      lastReceive = Some(request)
      Stream.empty
    }
    override def send(request: SendMessageRequest): SendMessageResponse = {
      lastSend = Some(request)
      SendMessageResponse.builder().messageId("sent").build()
    }
    override def delete(msg: SqsMessage): DeleteMessageResponse =
      DeleteMessageResponse.builder().build()
    override def resetVisibility(msg: SqsMessage): ChangeMessageVisibilityResponse =
      ChangeMessageVisibilityResponse.builder().build()
  }

  test("5.receive(builder) applies the builder and delegates to receive(request)") {
    val sqs = new CapturingSqs
    sqs.receive(_.queueUrl("my-queue").maxNumberOfMessages(3)).compile.drain
    assert(sqs.lastReceive.map(_.queueUrl()).contains("my-queue"))
    assert(sqs.lastReceive.flatMap(r => Option(r.maxNumberOfMessages())).map(_.intValue).contains(3))
  }

  test("6.send(builder) applies the builder and delegates to send(request)") {
    val sqs = new CapturingSqs
    val resp = sqs.send(_.queueUrl("my-queue").messageBody("hello"))
    assert(sqs.lastSend.map(_.queueUrl()).contains("my-queue"))
    assert(sqs.lastSend.map(_.messageBody()).contains("hello"))
    assert(resp.messageId() == "sent")
  }
}
