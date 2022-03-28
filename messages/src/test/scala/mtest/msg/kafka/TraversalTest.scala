package mtest.msg.kafka

import akka.kafka.ProducerMessage.MultiMessage as AkkaMultiMessage
import cats.effect.IO
import com.github.chenharryhua.nanjin.messages.kafka.*
import com.github.chenharryhua.nanjin.messages.kafka.instances.*
import fs2.kafka.{
  CommittableProducerRecords as Fs2CommittableProducerRecords,
  ProducerRecords as Fs2ProducerRecords,
  TransactionalProducerRecords as Fs2TransactionalProducerRecords
}
import monocle.law.discipline.TraversalTests
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline
class TraversalTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {
  import ArbitraryData.*
  checkAll(
    "akka.producer.MultiMessage",
    TraversalTests(BitraverseMessages[AkkaMultiMessage[*, *, String]].traversal[Int, Int, Int, Int]))

//  checkAll(
//    "fs2.producer.ProducerRecords",
//    TraversalTests(BitraverseMessages[Fs2ProducerRecords[*, *]].traversal[Int, Int, Int, Int]))

  checkAll(
    "fs2.producer.CommittableProducerRecords",
    TraversalTests(BitraverseMessages[Fs2CommittableProducerRecords[IO, *, *]].traversal[Int, Int, Int, Int])
  )

//  checkAll(
//    "fs2.producer.TransactionalProducerRecords",
//    TraversalTests(BitraverseMessages[Fs2TransactionalProducerRecords[IO, String, *, *]].traversal[Int, Int, Int, Int])
//  )
}
