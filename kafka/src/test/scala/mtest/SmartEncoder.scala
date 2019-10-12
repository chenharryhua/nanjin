package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.codec.KafkaGenericEncoder
import com.github.chenharryhua.nanjin.kafka.TopicDef
import fs2.kafka.{CommittableOffset, ProducerRecords, ProducerRecord => Fs2ProducerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import cats.implicits._

class SmartEncoder {
  val smart = TopicDef[Int, String]("smart-encoder").in(ctx)

  val e1: ProducerRecords[Int, String, Option[CommittableOffset[IO]]] =
    smart.encode[ProducerRecords[*, *, Option[CommittableOffset[IO]]]](1, "a")

  val e2: ProducerRecord[Int, String] =
    smart.encode[ProducerRecord](2, "b")
}
