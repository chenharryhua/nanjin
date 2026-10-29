package com.github.chenharryhua.nanjin.guard.service

import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.config.Capacity
import munit.CatsEffectSuite

class HistoryTest extends CatsEffectSuite {

  test("1.bounded history evicts the oldest value and clear removes the remainder") {
    for {
      history <- History[IO, Int](Some(Capacity(2)))
      _ <- history.add(1)
      _ <- history.add(2)
      _ <- history.add(3)
      retained <- history.value
      _ <- history.clear
      cleared <- history.value
    } yield {
      assertEquals(retained, Vector(2, 3))
      assertEquals(cleared, Vector.empty)
    }
  }

  test("2.capacity one always retains only the latest value") {
    for {
      history <- History[IO, String](Some(Capacity(1)))
      _ <- history.add("first")
      _ <- history.add("second")
      retained <- history.value
    } yield assertEquals(retained, Vector("second"))
  }

  test("3.absent and non-positive capacities produce no-op histories") {
    def verify(history: History[IO, Int]): IO[Unit] =
      history.add(1) >> history.clear >> history.value.map(values => assertEquals(values, Vector.empty))

    for {
      absent <- History[IO, Int](None)
      zero <- History[IO, Int](Some(Capacity(0)))
      negative <- History[IO, Int](Some(Capacity(-1)))
      _ <- verify(absent)
      _ <- verify(zero)
      _ <- verify(negative)
    } yield ()
  }
}
