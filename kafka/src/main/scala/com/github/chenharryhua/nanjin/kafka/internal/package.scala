package com.github.chenharryhua.nanjin.kafka

package object internal {
  type ConsumerActionM[A] = cats.free.Free[ConsumerActionF, A]
  type ConsumerActionA[A] = cats.free.FreeApplicative[ConsumerActionF, A]
}
