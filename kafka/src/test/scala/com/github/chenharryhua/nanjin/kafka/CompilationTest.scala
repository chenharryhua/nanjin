package com.github.chenharryhua.nanjin.kafka

import akka.kafka.ConsumerMessage
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Source
import org.scalatest.FunSuite

import scala.util.Try

class CompilationTest extends FunSuite {
  test("should compile") {
    val topic = ctx.topic[Int, Int]("do-not-run")
    topic.akkaResource.use { chn =>
      val ret: Source[ConsumerMessage.CommittableMessage[Int, Int], Consumer.Control] =
        chn.consume.map(chn.decode)
      ret.runWith(chn.ignoreSink)(chn.materializer)
    }
    topic.akkaResource.use { chn =>
      val ret: Source[Try[ConsumerMessage.CommittableMessage[Int, Int]], Consumer.Control] =
        chn.consume.map(chn.safeDecode)
      ret.runWith(chn.ignoreSink)(chn.materializer)
    }
    topic.akkaResource.use { chn =>
      val ret: Source[ConsumerMessage.CommittableMessage[Int, Array[Byte]], Consumer.Control] =
        chn.consume.map(chn.decodeKey)
      ret.runWith(chn.ignoreSink)(chn.materializer)
    }
    topic.akkaResource.use { chn =>
      val ret: Source[Try[ConsumerMessage.CommittableMessage[Int, Array[Byte]]], Consumer.Control] =
        chn.consume.map(chn.safeDecodeKey)
      ret.runWith(chn.ignoreSink)(chn.materializer)
    }
    topic.akkaResource.use { chn =>
      val ret: Source[ConsumerMessage.CommittableMessage[Array[Byte], Int], Consumer.Control] =
        chn.consume.map(chn.decodeValue)
      ret.runWith(chn.ignoreSink)(chn.materializer)
    }
    topic.akkaResource.use { chn =>
      val ret: Source[Try[ConsumerMessage.CommittableMessage[Array[Byte], Int]], Consumer.Control] =
        chn.consume.map(chn.safeDecodeValue)
      ret.runWith(chn.ignoreSink)(chn.materializer)
    }
    topic.akkaResource.use { chn =>
      val ret: Source[ConsumerMessage.CommittableMessage[Try[Int], Try[Int]], Consumer.Control] =
        chn.consume.map(chn.safeDecodeKeyValue)
      ret.runWith(chn.ignoreSink)(chn.materializer)
    }
  }
}
