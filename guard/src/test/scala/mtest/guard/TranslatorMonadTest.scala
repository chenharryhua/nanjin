package mtest.guard

import cats.Eval
import cats.effect.IO
import cats.kernel.Eq
import cats.laws.discipline.*
import cats.laws.discipline.SemigroupalTests.Isomorphisms
import cats.laws.discipline.eq.*
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, ServiceStart, ServiceStatus}
import com.github.chenharryhua.nanjin.guard.translators.Translator
import munit.DisciplineSuite

import java.time.{Instant, ZonedDateTime}
import java.util.UUID
import cats.*
import cats.laws.discipline.arbitrary.*
import cats.syntax.all.*
import org.scalacheck.{Cogen, Gen}
import org.scalacheck.Prop.*

object gendata {
  implicit val cogenEvent: Cogen[NJEvent] = Cogen[NJEvent]((e: NJEvent) => e.timestamp.toEpochMilli)

  val f1 = Gen.function1[NJEvent, Int]

}

class TranslatorMonadTest extends DisciplineSuite {
  import gendata.*
  val service = TaskGuard[IO]("monad").service("tailrecM")
  implicit val exhaustiveCheck: ExhaustiveCheck[NJEvent] =
    ExhaustiveCheck.instance(
      List(ServiceStart(ServiceStatus.Up(UUID.randomUUID(), Instant.now()), Instant.now(), service.serviceParams)))

  implicit def translatorEq: Eq[Translator[Eval, Int]] =
    Eq.by[Translator[Eval, Int], NJEvent => Eval[Option[Int]]](_.translate)

  implicit val iso: Isomorphisms[Translator[Eval, *]] = Isomorphisms.invariant[Translator[Eval, *]]

  // checkAll("Translator.MonadLaws", MonadTests[Translator[Eval, *]].monad[Int, Int, Int])

  test("test") {
    val ggg = f1.sample.map(_(Gen.posNum[Int]))
  }

}
