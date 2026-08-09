package com.github.chenharryhua.nanjin.aws

import cats.Endo
import cats.effect.kernel.{Resource, Sync}
import cats.implicits.catsSyntaxFlatMapOps
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.s3.model.{
  HeadObjectRequest,
  HeadObjectResponse,
  RenameObjectRequest,
  RenameObjectResponse
}
import software.amazon.awssdk.services.s3.{S3Client, S3ClientBuilder}

trait SimpleStorageService[F[_]] {
  def headObject(hor: HeadObjectRequest): F[HeadObjectResponse]
  final def headObject(f: Endo[HeadObjectRequest.Builder]): F[HeadObjectResponse] =
    headObject(f(HeadObjectRequest.builder()).build())

  def renameObject(ror: RenameObjectRequest): F[RenameObjectResponse]
  final def renameObject(f: Endo[RenameObjectRequest.Builder]): F[RenameObjectResponse] =
    renameObject(f(RenameObjectRequest.builder()).build())
}

object SimpleStorageService:
  private val name = "aws.s3"

  def apply[F[_]](f: Endo[S3ClientBuilder])(using F: Sync[F]): Resource[F, SimpleStorageService[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      client <- Resource.make(logger.info(s"initialize $name") >> F.blocking(f(S3Client.builder()).build())) {
        client => shutdown(name, logger)(client.close())
      }
    } yield new AwsS3[F](client, logger)

  final private class AwsS3[F[_]](client: S3Client, logger: Logger[F])(using F: Sync[F])
      extends SimpleStorageService[F] {

    override def headObject(hor: HeadObjectRequest): F[HeadObjectResponse] =
      blockingF(client.headObject(hor), hor.toString, logger)

    override def renameObject(ror: RenameObjectRequest): F[RenameObjectResponse] =
      blockingF(client.renameObject(ror), ror.toString, logger)
  }
end SimpleStorageService
