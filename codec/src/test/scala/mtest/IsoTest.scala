package mtest

import cats.Eq
import cats.implicits._
import com.github.chenharryhua.nanjin.codec.SerdeOf
import monocle.law.discipline.PrismTests
import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class IsoTest extends AnyFunSuite with Discipline {}
