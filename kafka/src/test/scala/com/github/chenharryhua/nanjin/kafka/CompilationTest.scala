package com.github.chenharryhua.nanjin.kafka

import akka.kafka.ConsumerMessage
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Source
import cats.implicits._
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Try

class CompilationTest extends AnyFunSuite {
  test("should compile") {
    val topic = ctx.topic[Int, Int]("do-not-run")
    val task = topic.akkaResource.use { chn =>
      val ret: Source[ConsumerMessage.CommittableMessage[Int, Int], Consumer.Control] =
        chn.consume.map(chn.messageDecoder.decode).take(0)
      ret.runWith(chn.ignoreSink)(chn.materializer)
    } >>
      topic.akkaResource.use { chn =>
        val ret: Source[Try[ConsumerMessage.CommittableMessage[Int, Int]], Consumer.Control] =
          chn.consume.map(chn.messageDecoder.tryDecode).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      } >>
      topic.akkaResource.use { chn =>
        val ret: Source[ConsumerMessage.CommittableMessage[Int, Array[Byte]], Consumer.Control] =
          chn.consume.map(chn.messageDecoder.decodeKey).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      } >>
      topic.akkaResource.use { chn =>
        val ret
          : Source[Try[ConsumerMessage.CommittableMessage[Int, Array[Byte]]], Consumer.Control] =
          chn.consume.map(chn.messageDecoder.tryDecodeKey).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      } >>
      topic.akkaResource.use { chn =>
        val ret: Source[ConsumerMessage.CommittableMessage[Array[Byte], Int], Consumer.Control] =
          chn.consume.map(chn.messageDecoder.decodeValue).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      } >>
      topic.akkaResource.use { chn =>
        val ret
          : Source[Try[ConsumerMessage.CommittableMessage[Array[Byte], Int]], Consumer.Control] =
          chn.consume.map(chn.messageDecoder.tryDecodeValue).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      } >>
      topic.akkaResource.use { chn =>
        val ret: Source[ConsumerMessage.CommittableMessage[Try[Int], Try[Int]], Consumer.Control] =
          chn.consume.map(chn.messageDecoder.tryDecodeKeyValue).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      }
    task.unsafeRunSync()
  }
}
