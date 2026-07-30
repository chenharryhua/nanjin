package com.github.chenharryhua.nanjin.guard.batch

final private case class MonadicExecutionState[A](eoa: Either[Throwable, A], history: List[CompletedJob]) {
  def update[B](ex: Throwable): MonadicExecutionState[B] = copy(eoa = Left(ex))

  // reversed order
  def prependHistory[B](js: MonadicExecutionState[B]): MonadicExecutionState[B] =
    MonadicExecutionState[B](js.eoa, js.history ::: history)

  def map[B](f: A => B): MonadicExecutionState[B] = copy(eoa = eoa.map(f))
}
