package com.github.chenharryhua.nanjin.guard.batch

final private case class ExecutionState[A](eoa: Either[Throwable, A], history: List[CompletedJob]) {
  def update[B](ex: Throwable): ExecutionState[B] = copy(eoa = Left(ex))

  // reversed order
  def prependHistory[B](js: ExecutionState[B]): ExecutionState[B] =
    ExecutionState[B](js.eoa, js.history ::: history)

  def map[B](f: A => B): ExecutionState[B] = copy(eoa = eoa.map(f))
}

final private case class JobNameIndex[F[_], A](name: String, index: Int, fa: F[A])
