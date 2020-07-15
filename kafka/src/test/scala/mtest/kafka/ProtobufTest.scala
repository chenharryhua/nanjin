package mtest.kafka

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.codec.KPB
import mtest.pb.test._
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class ProtobufTest extends AnyFunSuite {

  val topic: KafkaTopic[IO, String, KPB[Person]] =
    ctx.topic[String, KPB[Person]]("producer.test.protobuf")
  ignore("producer protobuf messsages") {

    val produceTask = (0 until 100).toList.traverse { i =>
      topic.send(i.toString, KPB(Person(s"$i", Random.nextInt(100))))
    }
    produceTask.unsafeRunSync
  }
}
