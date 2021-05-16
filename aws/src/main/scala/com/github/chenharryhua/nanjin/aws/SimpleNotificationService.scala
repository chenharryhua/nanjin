package com.github.chenharryhua.nanjin.aws

import akka.actor.ActorSystem
import cats.effect.Async

class SimpleNotificationService[F[_]](akkaSystem: ActorSystem)(implicit F: Async[F]) {}
