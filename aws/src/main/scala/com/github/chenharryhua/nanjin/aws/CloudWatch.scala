package com.github.chenharryhua.nanjin.aws

import cats.Endo
import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.all.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.cloudwatch.{CloudWatchClient, CloudWatchClientBuilder}
import software.amazon.awssdk.services.cloudwatch.model.{PutMetricDataRequest, PutMetricDataResponse}

/** A simplified Cats Effect wrapper for AWS CloudWatch.
  *
  * Provides methods to send metrics to CloudWatch with resource-safe client management and built-in logging.
  *
  * @tparam F
  *   effect type, e.g., cats.effect.IO
  */
trait CloudWatch[F[_]] {

  /** Sends a CloudWatch metric using the given request.
    *
    * @param request
    *   the AWS `PutMetricDataRequest` to send
    * @return
    *   the AWS `PutMetricDataResponse` wrapped in F
    */
  def putMetricData(request: PutMetricDataRequest): F[PutMetricDataResponse]

  /** Sends a CloudWatch metric using a builder function.
    *
    * Convenience method to construct a `PutMetricDataRequest` using a builder function.
    *
    * @param f
    *   function to modify a `PutMetricDataRequest.Builder`
    * @return
    *   the AWS `PutMetricDataResponse` wrapped in F
    */
  final def putMetricData(f: Endo[PutMetricDataRequest.Builder]): F[PutMetricDataResponse] =
    putMetricData(f(PutMetricDataRequest.builder()).build())
}

object CloudWatch {

  private val name: String = "aws.CloudWatch"

  /** Creates a managed CloudWatch client.
    *
    * The returned `Resource` ensures proper client shutdown and logging.
    *
    * @param f
    *   function to modify a `CloudWatchClientBuilder`
    * @param F
    *   async effect type
    * @return
    *   a resource wrapping `CloudWatch` implementation
    */
  def apply[F[_]](f: Endo[CloudWatchClientBuilder])(implicit F: Async[F]): Resource[F, CloudWatch[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      client <- Resource.make(logger.info(s"initialize $name").as(f(CloudWatchClient.builder()).build())) {
        cw =>
          shutdown(name, logger)(F.blocking(cw.close()))
      }
    } yield new AwsCloudWatch[F](client, logger)

  final private class AwsCloudWatch[F[_]](client: CloudWatchClient, logger: Logger[F])(implicit F: Sync[F])
      extends CloudWatch[F] {

    override def putMetricData(request: PutMetricDataRequest): F[PutMetricDataResponse] =
      blockingF(client.putMetricData(request), request.toString, logger)
  }
}
