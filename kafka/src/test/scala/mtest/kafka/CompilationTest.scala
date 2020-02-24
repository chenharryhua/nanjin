package mtest.kafka

import akka.kafka.ConsumerMessage
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Source
import org.scalatest.funsuite.AnyFunSuite
import cats.implicits._

import scala.util.Try
import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.KafkaTopic

class CompilationTest extends AnyFunSuite {
  val topic: KafkaTopic[IO, Int, Int] = ctx.topic[Int, Int]("compilation.test")
  test("should compile") {
    val chn = topic.akkaChannel(akkaSystem)
    val task =
      topic.admin.IdefinitelyWantToDeleteTheTopicAndUnderstandItsConsequence >>
        topic.schemaRegistry.register >>
        topic.send(List.fill(10)(topic.fs2PR(1, 1))) >> {
        val ret: Source[ConsumerMessage.CommittableMessage[Int, Int], Consumer.Control] =
          chn.consume.map(m => topic.decoder(m).decode).take(1)
        ret.runWith(chn.ignoreSink)(materializer) *> IO(println("-----------1-------------"))
      } >> {
        val ret: Source[Try[ConsumerMessage.CommittableMessage[Int, Int]], Consumer.Control] =
          chn.consume.map(m => topic.decoder(m).tryDecode).take(1)
        ret.runWith(chn.ignoreSink)(materializer) *> IO(println("-----------2-------------"))
      } >> {
        val ret: Source[ConsumerMessage.CommittableMessage[Int, Array[Byte]], Consumer.Control] =
          chn.consume.map(m => topic.decoder(m).decodeKey).take(1)
        ret.runWith(chn.ignoreSink)(materializer) *> IO(println("-----------3-------------"))
      } >> {
        val ret
          : Source[Try[ConsumerMessage.CommittableMessage[Int, Array[Byte]]], Consumer.Control] =
          chn.consume.map(m => topic.decoder(m).tryDecodeKey).take(1)
        ret.runWith(chn.ignoreSink)(materializer) *> IO(println("-----------4-------------"))
      } >> {
        val ret: Source[ConsumerMessage.CommittableMessage[Array[Byte], Int], Consumer.Control] =
          chn.consume.map(m => topic.decoder(m).decodeValue).take(1)
        ret.runWith(chn.ignoreSink)(materializer) *> IO(println("-----------5-------------"))
      } >> {
        val ret
          : Source[Try[ConsumerMessage.CommittableMessage[Array[Byte], Int]], Consumer.Control] =
          chn.consume.map(m => topic.decoder(m).tryDecodeValue).take(1)
        ret.runWith(chn.ignoreSink)(materializer) *> IO(println("-----------6-------------"))
      } >> {
        val ret: Source[ConsumerMessage.CommittableMessage[Try[Int], Try[Int]], Consumer.Control] =
          chn.consume.map(m => topic.decoder(m).tryDecodeKeyValue).take(1)
        ret.runWith(chn.ignoreSink)(materializer) *> IO(println("-----------7-------------"))
      }
    task.unsafeRunSync()
  }
}
