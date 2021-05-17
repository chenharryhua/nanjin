package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import cats.syntax.all._
import com.amazonaws.regions.Regions
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import io.circe.syntax._

sealed trait AlertService[F[_]] {
  def alert(msg: SlackNotification)(implicit F: Async[F]): F[Unit]
}

object AlertService {

  def fake[F[_]]: AlertService[F] = new AlertService[F] {

    override def alert(msg: SlackNotification)(implicit F: Async[F]): F[Unit] = F.unit
  }

  def sns[F[_]](service: SimpleNotificationService[F]): AlertService[F] =
    new AlertService[F] {

      override def alert(msg: SlackNotification)(implicit F: Async[F]): F[Unit] =
        service.publish(msg.asJson.noSpaces).attempt.void
    }

  def sns[F[_]](topic: SnsArn, region: Regions): AlertService[F] =
    sns[F](SimpleNotificationService(topic, region))
}
