package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import cats.effect.syntax.all._
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.action.ActionGuard
import com.github.chenharryhua.nanjin.guard.alert.NJEvent
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, GroupConfig, GroupParams}
import fs2.Stream
import fs2.concurrent.Topic
import retry.RetryPolicies
import scala.concurrent.duration._

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

  def eventStream[A](actionGuard: (String => ActionGuard[F]) => F[A])(implicit F: Async[F]): Stream[F, NJEvent] =
    Stream.eval(Topic[F, NJEvent]).flatMap { topic =>
      val subscriber = topic.subscribe(params.topicMaxQueued)
      val publisher = Stream.eval {
        val ret = retry.retryingOnAllErrors[A](
          params.retryPolicy.policy[F].join(RetryPolicies.limitRetries(params.maxRetries)),
          retry.noop[F, Throwable]
        ) {
          actionGuard(actionName => new ActionGuard[F](topic, applicationName, groupName, actionName, actionConfig))
        }
        ret.guarantee(topic.close.void)
      }
      subscriber.concurrently(publisher.delayBy(10.milliseconds))
    }
}
