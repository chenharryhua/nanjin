package mtest.kafka

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.codec.KPB
import mtest.kafka.pb.test._
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class ProtobufTest extends AnyFunSuite {

  val topic: KafkaTopic[IO, String, KPB[PersonKafka]] =
    ctx.topic[String, KPB[PersonKafka]]("producer.test.protobuf")
  ignore("producer protobuf messsages") {}
}
