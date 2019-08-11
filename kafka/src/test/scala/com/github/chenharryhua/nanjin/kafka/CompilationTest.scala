package com.github.chenharryhua.nanjin.kafka

import akka.kafka.ConsumerMessage
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Source
import org.scalatest.FunSuite
import cats.implicits._

import scala.util.Try

class CompilationTest extends FunSuite {
  test("should compile") {
    val topic = ctx.topic[Int, Int]("do-not-run")
    val task = topic.akkaResource.use { chn =>
      val ret: Source[ConsumerMessage.CommittableMessage[Int, Int], Consumer.Control] =
        chn.consume.map(chn.decode).take(0)
      ret.runWith(chn.ignoreSink)(chn.materializer)
    } >>
      topic.akkaResource.use { chn =>
        val ret: Source[Try[ConsumerMessage.CommittableMessage[Int, Int]], Consumer.Control] =
          chn.consume.map(chn.safeDecode).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      } >>
      topic.akkaResource.use { chn =>
        val ret: Source[ConsumerMessage.CommittableMessage[Int, Array[Byte]], Consumer.Control] =
          chn.consume.map(chn.decodeKey).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      } >>
      topic.akkaResource.use { chn =>
        val ret
          : Source[Try[ConsumerMessage.CommittableMessage[Int, Array[Byte]]], Consumer.Control] =
          chn.consume.map(chn.safeDecodeKey).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      } >>
      topic.akkaResource.use { chn =>
        val ret: Source[ConsumerMessage.CommittableMessage[Array[Byte], Int], Consumer.Control] =
          chn.consume.map(chn.decodeValue).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      } >>
      topic.akkaResource.use { chn =>
        val ret
          : Source[Try[ConsumerMessage.CommittableMessage[Array[Byte], Int]], Consumer.Control] =
          chn.consume.map(chn.safeDecodeValue).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      } >>
      topic.akkaResource.use { chn =>
        val ret: Source[ConsumerMessage.CommittableMessage[Try[Int], Try[Int]], Consumer.Control] =
          chn.consume.map(chn.safeDecodeKeyValue).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      }
    task.unsafeRunSync()
  }
}
