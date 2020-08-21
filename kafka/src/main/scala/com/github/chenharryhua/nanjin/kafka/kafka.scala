package com.github.chenharryhua.nanjin

import akka.actor.ActorSystem
import cats.effect.{Async, ContextShift, Resource}
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

  def akkaResource[F[_]: ContextShift](implicit F: Async[F]): Resource[F, ActorSystem] =
    Resource.make(F.delay(ActorSystem("nj-akka")))(a =>
      Async.fromFuture(F.delay(a.terminate().map(_ => ())(a.dispatcher))))

  def akkaStream[F[_]: ContextShift: Async]: Stream[F, ActorSystem] =
    Stream.resource(akkaResource[F])

}
