package com.github.chenharryhua.nanjin.aws

final case class SlackField(title: String, value: String, short: Boolean)
final case class Attachment(color: String, title: String, fields: List[SlackField])

final case class SlackNotification(
  username: String,
  text: String,
  attachments: List[Attachment],
  channel: Option[String] = None)
