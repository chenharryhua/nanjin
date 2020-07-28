package com.github.chenharryhua.nanjin

import akka.actor.ActorSystem
import cats.effect.{Async, Blocker, ContextShift, Resource}
import cats.implicits._
import eu.timepit.refined.W
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.string.MatchesRegex
import fs2.Stream
package object kafka extends ShowKafkaMessage {

  type TopicName = String Refined MatchesRegex[W.`"^[a-zA-Z0-9_.-]+$"`.T]

  object TopicName extends RefinedTypeOps[TopicName, String] with CatsRefinedTypeOpsSyntax

  type StoreName = String Refined MatchesRegex[W.`"^[a-zA-Z0-9_.-]+$"`.T]

  object StoreName extends RefinedTypeOps[StoreName, String] with CatsRefinedTypeOpsSyntax

  def akkaResource[F[_]: ContextShift: Async](blocker: Blocker): Resource[F, ActorSystem] =
    Resource.make(blocker.delay(ActorSystem("nj-akka")))(a =>
      Async.fromFuture(blocker.delay(a.terminate().map(_ => ())(blocker.blockingContext))))

  def akkaStream[F[_]: ContextShift: Async](blocker: Blocker): Stream[F, ActorSystem] =
    Stream.resource(akkaResource[F](blocker))

}
