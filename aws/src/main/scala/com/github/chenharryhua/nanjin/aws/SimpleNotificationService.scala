package com.github.chenharryhua.nanjin.aws

import akka.actor.ActorSystem
import cats.effect.Async

final case class SlackField(title: String, value: String, short: Boolean)
final case class Attachment(color: String, title: String, fields: List[SlackField])

final case class SlackNotification(
  username: String,
  text: String,
  attachments: List[Attachment],
  channel: Option[String] = None)

class SimpleNotificationService[F[_]](akkaSystem: ActorSystem)(implicit F: Async[F]) {}
