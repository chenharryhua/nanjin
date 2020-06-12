package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.TopicDef
import org.scalatest.funsuite.AnyFunSuite
import io.circe.generic._
import io.circe.shapes._
import shapeless.{:+:, CNil, Coproduct}
import com.github.chenharryhua.nanjin.kafka.TopicName

object CoproductCase {
  @JsonCodec sealed trait SealedTrait
  final case object CaseObject extends SealedTrait
  final case class CaseClass(a: Int, b: String) extends SealedTrait

  type PrimCops = Int :+: String :+: Long :+: Double :+: Float :+: CNil
  @JsonCodec final case class PC(a: PrimCops, b: PrimCops, c: Int)

  @JsonCodec final case class C1(a: Int)
  @JsonCodec final case class C2(a1: Int, b1: String)
  @JsonCodec final case class C3(a2: String, b2: Float, c3: Int)

  @JsonCodec final case class CompositeT(c1: C1, c2: C2, c3: C3)

  type ComplexType = C1 :+: C2 :+: C3 :+: CNil
  @JsonCodec final case class CT(ct: ComplexType, a: Int)

}

class MessageCoproductTest extends AnyFunSuite {
  import CoproductCase._
  test("coproduct in terms of sealed trait") {
    val topic = TopicDef[Int, SealedTrait](TopicName("message.coproduct.trait.test")).in(ctx)
    val m     = CaseClass(1, "b")
    val rst = for {
      _ <- topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- topic.schemaDelete
      _ <- topic.send(0, m)
      r <- topic.shortLiveConsumer.use(_.retrieveLastRecords)
    } yield assert(topic.decoder(r.head).decodeValue.value() === m)
    rst.unsafeRunSync()
  }

  test("coproduct of primitive types") {
    val topic = TopicDef[Int, PC](TopicName("message.coproduct.primitives.test")).in(ctx)
    val m     = PC(Coproduct[PrimCops](20121026.000001f), Coproduct[PrimCops]("aaa"), 1)
    val rst = for {
      _ <- topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- topic.schemaDelete
      _ <- topic.send(0, m)
      r <- topic.shortLiveConsumer.use(_.retrieveLastRecords)
    } yield assert(topic.decoder(r.head).decodeValue.value() === m)
    rst.unsafeRunSync()
  }

  test("composite types") {
    val topic = TopicDef[Int, CompositeT](TopicName("message.composite.test")).in(ctx)
    val m     = CompositeT(C1(1), C2(1, "a"), C3("a", 1.0f, 1))
    val rst = for {
      _ <- topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- topic.schemaDelete
      _ <- topic.send(0, m)
      r <- topic.shortLiveConsumer.use(_.retrieveLastRecords)
    } yield assert(topic.decoder(r.head).decodeValue.value() === m)
    rst.unsafeRunSync()
  }

  test("coproduct of composite types") {
    val topic = TopicDef[Int, CT](TopicName("message.coproduct.composite.test")).in(ctx)

    val m1 = CT(Coproduct[ComplexType](C1(1)), 1)
    val m2 = CT(Coproduct[ComplexType](C2(1, "s")), 2)
    val m3 = CT(Coproduct[ComplexType](C3("s", 1.0f, 1)), 3)

    val rst = for {
      _ <- topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- topic.schemaDelete
      _ <- topic.send(1, m1)
      r1 <- topic.shortLiveConsumer.use(_.retrieveLastRecords)
      _ <- topic.send(2, m2)
      r2 <- topic.shortLiveConsumer.use(_.retrieveLastRecords)
      _ <- topic.send(3, m3)
      r3 <- topic.shortLiveConsumer.use(_.retrieveLastRecords)

    } yield {
      assert(topic.decoder(r1.head).decodeValue.value() === m1)
      assert(topic.decoder(r2.head).decodeValue.value() === m2)
      assert(topic.decoder(r3.head).decodeValue.value() === m3)
    }
    rst.unsafeRunSync()
  }
}
