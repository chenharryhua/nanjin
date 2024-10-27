package mtest.guard

import cats.Eq
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.laws.discipline.eq.*
import cats.laws.discipline.{ExhaustiveCheck, FunctorFilterTests, MonadTests}
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.chrono.{Policy, TickStatus}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import com.github.chenharryhua.nanjin.guard.translator.Translator
import munit.DisciplineSuite
import org.scalacheck.{Arbitrary, Gen}
// TODO

object gendata {
  val service: ServiceGuard[IO] = TaskGuard[IO]("monad").service("tailrecM")
  val tick                      = TickStatus.zeroth[IO](Policy.giveUp, sydneyTime).unsafeRunSync().tick
  implicit val exhaustiveCheck: ExhaustiveCheck[NJEvent] =
    ExhaustiveCheck.instance(List(ServiceStart(null.asInstanceOf[ServiceParams], tick)))

  implicit def translatorEq: Eq[Translator[Option, Int]] =
    Eq.by[Translator[Option, Int], NJEvent => Option[Option[Int]]](_.translate)

  implicit val arbiTranslator: Arbitrary[Translator[Option, Int]] =
    Arbitrary(
      Gen.const(
        Translator
          .empty[Option, Int]
          .withServiceStart(_ => 1)
          .withServiceStop(_ => 2)
          .withServicePanic(_ => 3)
          .withMetricReport(_ => 4)
          .withMetricReset(_ => 5)
          .withServiceMessage(_ => 6)
          .withActionStart(_ => 8)
          .withActionFail(_ => 9)
          .withActionDone(_ => 10)
          .withActionRetry(_ => 11)))

  val add: Int => Int = _ + 1

  implicit val arbiAtoB: Arbitrary[Translator[Option, Int => Int]] =
    Arbitrary(
      Gen.const(
        Translator
          .empty[Option, Int => Int]
          .withServiceStart(_ => add)
          .withServiceStop(_ => add)
          .withServicePanic(_ => add)
          .withMetricReport(_ => add)
          .withMetricReset(_ => add)
          .withServiceMessage(_ => add)
          .withActionStart(_ => add)
          .withActionFail(_ => add)
          .withActionDone(_ => add)
          .withActionRetry(_ => add)))

  implicit val eqAbc: Eq[Translator[Option, (Int, Int, Int)]] =
    (_: Translator[Option, (Int, Int, Int)], _: Translator[Option, (Int, Int, Int)]) => true

  implicit val arbFOA: Arbitrary[Translator[Option, Option[Int]]] =
    Arbitrary(
      Gen.const(
        Translator
          .empty[Option, Option[Int]]
          .withServiceStart(_ => Option(1))
          .withServiceStop(_ => Option(2))
          .withServicePanic(_ => Option(3))
          .withMetricReport(_ => Option(4))
          .withMetricReset(_ => Option(5))
          .withServiceMessage(_ => Option(6))
          .withActionStart(_ => Option(8))
          .withActionFail(_ => Option(9))
          .withActionDone(_ => Option(10))
          .withActionRetry(_ => Option(11))))
}

class TranslatorMonadTest extends DisciplineSuite {
  import gendata.*
  // just check tailRecM stack safety
  checkAll("Translator.MonadLaws", MonadTests[Translator[Option, *]].monad[Int, Int, Int])
  checkAll("Translator.FunctorFilter", FunctorFilterTests[Translator[Option, *]].functorFilter[Int, Int, Int])

}
