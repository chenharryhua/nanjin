package com.github.chenharryhua.nanjin.guard.batch

import io.circe.Json

trait JobHandler[A] { outer =>
  def predicate(a: A): Boolean
  def translate(a: A, jrs: JobResultState): Json

  final def contramap[B](f: B => A): JobHandler[B] =
    new JobHandler[B] {
      override def predicate(b: B): Boolean = outer.predicate(f(b))
      override def translate(b: B, jrs: JobResultState): Json =
        outer.translate(f(b), jrs)
    }

  final def withPredicate(f: A => Boolean): JobHandler[A] =
    new JobHandler[A] {
      override def predicate(a: A): Boolean = f(a)
      override def translate(a: A, jrs: JobResultState): Json = outer.translate(a, jrs)
    }
}
