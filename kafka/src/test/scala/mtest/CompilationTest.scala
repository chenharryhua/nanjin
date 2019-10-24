package mtest

import akka.kafka.ConsumerMessage
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Source
import org.scalatest.funsuite.AnyFunSuite
import scala.util.Try
import cats.implicits._
import com.github.chenharryhua.nanjin.codec._
import com.github.chenharryhua.nanjin.codec.bitraverse._
class CompilationTest extends AnyFunSuite {
  test("should compile") {
    val topic = ctx.topic[Int, Int]("do-not-run")
    val task = topic.akkaResource.use { chn =>
      val ret: Source[ConsumerMessage.CommittableMessage[Int, Int], Consumer.Control] =
        chn.consume.map(m => topic.decoder(m).decode).take(0)
      ret.runWith(chn.ignoreSink)(chn.materializer)
    } >>
      topic.akkaResource.use { chn =>
        val ret: Source[Try[ConsumerMessage.CommittableMessage[Int, Int]], Consumer.Control] =
          chn.consume.map(m => topic.decoder(m).tryDecode).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      } >>
      topic.akkaResource.use { chn =>
        val ret: Source[ConsumerMessage.CommittableMessage[Int, Array[Byte]], Consumer.Control] =
          chn.consume.map(m => topic.decoder(m).decodeKey).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      } >>
      topic.akkaResource.use { chn =>
        val ret
          : Source[Try[ConsumerMessage.CommittableMessage[Int, Array[Byte]]], Consumer.Control] =
          chn.consume.map(m => topic.decoder(m).tryDecodeKey).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      } >>
      topic.akkaResource.use { chn =>
        val ret: Source[ConsumerMessage.CommittableMessage[Array[Byte], Int], Consumer.Control] =
          chn.consume.map(m => topic.decoder(m).decodeValue).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      } >>
      topic.akkaResource.use { chn =>
        val ret
          : Source[Try[ConsumerMessage.CommittableMessage[Array[Byte], Int]], Consumer.Control] =
          chn.consume.map(m => topic.decoder(m).tryDecodeValue).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      } >>
      topic.akkaResource.use { chn =>
        val ret: Source[ConsumerMessage.CommittableMessage[Try[Int], Try[Int]], Consumer.Control] =
          chn.consume.map(m => topic.decoder(m).tryDecodeKeyValue).take(0)
        ret.runWith(chn.ignoreSink)(chn.materializer)
      }
    task.unsafeRunSync()
  }
}
