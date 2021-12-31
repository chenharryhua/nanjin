package mtest.guard

import cats.effect.IO
import cats.laws.discipline.MonadTests
import com.github.chenharryhua.nanjin.guard.translators.Translator
import io.circe.Json
import munit.DisciplineSuite
class TranslatorMonadTest extends DisciplineSuite {

  // checkAll("Translator.MonadLaws", MonadTests[Translator[IO, *]].monad[Json, Json, Json])

}
