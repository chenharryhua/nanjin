package mtest
import akka.kafka.ProducerMessage.{MultiMessage => AkkaMultiMessage}
import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.codec.LikeProducerRecords
import fs2.kafka.{
  CommittableProducerRecords   => Fs2CommittableProducerRecords,
  ProducerRecords              => Fs2ProducerRecords,
  TransactionalProducerRecords => Fs2TransactionalProducerRecords
}
import monocle.law.discipline.TraversalTests
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class TraversalTest extends AnyFunSuite with Discipline {
  checkAll(
    "akka.producer.MultiMessage",
    TraversalTests(
      LikeProducerRecords[AkkaMultiMessage[*, *, String]].traversal[Int, Int, Int, Int]))

  checkAll(
    "fs2.producer.ProducerRecords",
    TraversalTests(
      LikeProducerRecords[Fs2ProducerRecords[*, *, String]].traversal[Int, Int, Int, Int]))

  checkAll(
    "fs2.producer.CommittableProducerRecords",
    TraversalTests(
      LikeProducerRecords[Fs2CommittableProducerRecords[IO, *, *]].traversal[Int, Int, Int, Int])
  )

  checkAll(
    "fs2.producer.TransactionalProducerRecords",
    TraversalTests(
      LikeProducerRecords[Fs2TransactionalProducerRecords[IO, *, *, String]]
        .traversal[Int, Int, Int, Int])
   )
}
