package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import io.circe.syntax._
import com.amazonaws.regions.Regions

trait AlertService[F[_]] {
  def alert(msg: SlackNotification)(implicit F: Async[F]): F[Unit]
  def alert(msg: LimitedRetryState)(implicit F: Async[F]): F[Unit]
}

object AlertService {

  def fake[F[_]]: AlertService[F] = new AlertService[F] {

    override def alert(msg: SlackNotification)(implicit F: Async[F]): F[Unit] = F.unit
    override def alert(msg: LimitedRetryState)(implicit F: Async[F]): F[Unit] = F.unit
  }

  def sns[F[_]](topic: SnsArn, region: Regions): AlertService[F] =
    new AlertService[F] {

      private val service: SimpleNotificationService[F] = SimpleNotificationService[F](topic, region)

      override def alert(msg: SlackNotification)(implicit F: Async[F]): F[Unit] =
        service.publish(msg.asJson.noSpaces)

      override def alert(msg: LimitedRetryState)(implicit F: Async[F]): F[Unit] =
        F.blocking(println(msg))
    }
}
