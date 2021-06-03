package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import fs2.concurrent.Topic
import retry.RetryPolicies

final class GroupGuard[F[_]](
  applicationName: String,
  groupName: String,
  groupConfig: GroupConfig,
  actionConfig: ActionConfig) {
  val params: GroupParams = groupConfig.evalConfig

  def updateGroupConfig(f: GroupConfig => GroupConfig): GroupGuard[F] =
    new GroupGuard[F](applicationName, groupName, f(groupConfig), actionConfig)

  def updateActionConfig(f: ActionConfig => ActionConfig): GroupGuard[F] =
    new GroupGuard[F](applicationName, groupName, groupConfig, f(actionConfig))

  def eventStream[A](actionGuard: (String => ActionGuard[F]) => F[A])(implicit F: Async[F]): Stream[F, NJEvent] = {
    val log = new LogService[F]()
    Stream
      .bracket(Topic[F, NJEvent])(_.close.void)
      .flatMap { topic =>
        val publisher = Stream.eval(
          retry.retryingOnAllErrors[A](
            params.retryPolicy.policy[F].join(RetryPolicies.limitRetries(params.maxRetries)),
            retry.noop[F, Throwable]
          ) {
            actionGuard(actionName => new ActionGuard[F](topic, applicationName, groupName, actionName, actionConfig))
          })
        val consumer: Stream[F, NJEvent] = topic.subscribe(params.topicMaxQueued)
        consumer.concurrently(publisher)
      }
      .evalTap(event => log.alert(event).whenA(params.isLogging))
  }
}
 