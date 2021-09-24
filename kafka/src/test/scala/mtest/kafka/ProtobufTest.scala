package mtest.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.codec.KPB
import mtest.pb.kafka.PersonKafka
import org.scalatest.funsuite.AnyFunSuite

class ProtobufTest extends AnyFunSuite {

  val topic: KafkaTopic[IO, String, KPB[PersonKafka]] =
    ctx.topic[String, KPB[PersonKafka]]("producer.test.protobuf")
  ignore("producer protobuf messsages") {}
}
