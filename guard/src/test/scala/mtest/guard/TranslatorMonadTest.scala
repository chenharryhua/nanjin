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

import java.time.ZonedDateTime
import java.util.UUID

class TranslatorMonadTest extends DisciplineSuite {
  val service = TaskGuard[IO]("monad").service("service")
  implicit val exhaustiveCheck: ExhaustiveCheck[NJEvent] =
    ExhaustiveCheck.instance(
      List(
        ServiceStart(
          ServiceStatus.Up(UUID.randomUUID(), ZonedDateTime.now()),
          ZonedDateTime.now(),
          service.serviceParams)))

  implicit def translatorEq(implicit ev: Eq[NJEvent => Eval[Int]]): Eq[Translator[Eval, Int]] =
    Eq.by[Translator[Eval, Int], NJEvent => Eval[Option[Int]]](_.translate)

  implicit val iso = Isomorphisms.invariant[Translator[Eval, *]]

  // checkAll("Translator.MonadLaws", MonadTests[Translator[Eval, *]].monad[Int, Int, Int])

}
