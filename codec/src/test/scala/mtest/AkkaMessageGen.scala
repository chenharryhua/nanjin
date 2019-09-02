package mtest
import java.util.concurrent.CompletionStage

import akka.Done
import akka.kafka.ConsumerMessage.{
  CommittableMessage,
  CommittableOffset,
  GroupTopicPartition,
  PartitionOffset
}
import akka.kafka.ProducerMessage.Message
import com.github.chenharryhua.nanjin.codec.BitraverseAkkaMessage
import org.scalacheck.Gen

import scala.concurrent.Future

trait AkkaMessageGen extends KafkaRawMessageGen with BitraverseAkkaMessage {

  val genGroupTopicPartition: Gen[GroupTopicPartition] = for {
    groupId <- Gen.asciiPrintableStr
    topic <- Gen.asciiPrintableStr
    partition <- Gen.posNum[Int]
  } yield GroupTopicPartition(groupId, topic, partition)

  val genAkkaConsumerMessage: Gen[CommittableMessage[Int, Int]] = for {
    cr <- genConsumerRecord
    gtp <- genGroupTopicPartition
    offset <- Gen.posNum[Long]
  } yield CommittableMessage(
    cr,
    new CommittableOffset {
      val partitionOffset: PartitionOffset                = new PartitionOffset(gtp, offset)
      override def commitScaladsl(): Future[Done]         = null
      override def commitJavadsl(): CompletionStage[Done] = null
      override def batchSize: Long                        = 0
    }
  )

  val genAkkaProducerMessage: Gen[Message[Int, Int, String]] = for {
    cr <- genProducerRecord
  } yield Message(cr, "pass-through")

}
