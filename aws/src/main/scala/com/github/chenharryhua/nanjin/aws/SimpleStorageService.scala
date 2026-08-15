package com.github.chenharryhua.nanjin.aws

import cats.Endo
import cats.effect.kernel.{Resource, Sync}
import cats.implicits.catsSyntaxFlatMapOps
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.{
  GetObjectRequest,
  GetObjectResponse,
  HeadObjectRequest,
  HeadObjectResponse,
  PutObjectRequest,
  PutObjectResponse,
  RenameObjectRequest,
  RenameObjectResponse
}
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.services.s3.presigner.model.{GetObjectPresignRequest, PresignedGetObjectRequest}
import software.amazon.awssdk.services.s3.{S3Client, S3ClientBuilder}

import java.net.URI
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.ScalaDurationOps

trait SimpleStorageService[F[_]] {

  /** Download an object using a fully configured S3 request.
    *
    * The resource closes the response stream after the caller finishes consuming it.
    */
  def getObject(gor: GetObjectRequest): Resource[F, ResponseInputStream[GetObjectResponse]]

  /** Download an object using a fluent S3 request builder. */
  final def getObject(
    f: Endo[GetObjectRequest.Builder]): Resource[F, ResponseInputStream[GetObjectResponse]] =
    getObject(f(GetObjectRequest.builder()).build())

  /** Upload an object using a fully configured request and its content body.
    *
    * The request controls object metadata and options; the supplied body provides the bytes sent to S3.
    */
  def putObject(body: RequestBody, por: PutObjectRequest): F[PutObjectResponse]

  /** Upload an object using a fluent S3 request builder and its content body. */
  final def putObject(body: RequestBody, f: Endo[PutObjectRequest.Builder]): F[PutObjectResponse] =
    putObject(body, f(PutObjectRequest.builder()).build())

  /** Retrieve object metadata using a fully configured S3 request.
    *
    * This checks object metadata without downloading the object. The operation requires the permissions and
    * endpoint configuration appropriate for the target bucket.
    */
  def headObject(hor: HeadObjectRequest): F[HeadObjectResponse]

  /** Retrieve object metadata using a fluent S3 request builder. */
  final def headObject(f: Endo[HeadObjectRequest.Builder]): F[HeadObjectResponse] =
    headObject(f(HeadObjectRequest.builder()).build())

  /** Rename an object using a fully configured S3 request.
    *
    * S3 rename is supported for S3 Express One Zone directory buckets. For standard S3 buckets, use a copy
    * followed by a delete operation when the client and permissions support those operations.
    */
  def renameObject(ror: RenameObjectRequest): F[RenameObjectResponse]

  /** Rename an object using a fluent S3 request builder. */
  final def renameObject(f: Endo[RenameObjectRequest.Builder]): F[RenameObjectResponse] =
    renameObject(f(RenameObjectRequest.builder()).build())

  /** Create a presigned GET request from a fully configured presign request.
    *
    * The returned request contains the signed URL and may be used by a caller or another HTTP client.
    */
  def presignGetObject(gpr: GetObjectPresignRequest): F[PresignedGetObjectRequest]

  /** Create a presigned GET request using a fluent presign request builder. */
  final def presignGetObject(f: Endo[GetObjectPresignRequest.Builder]): F[PresignedGetObjectRequest] =
    presignGetObject(f(GetObjectPresignRequest.builder()).build())

  /** Create a presigned GET request for a validated `s3://bucket/key` URL.
    *
    * This convenience overload is intended for development and one-off use; do not use it in production.
    * Production code should use the fully configured `GetObjectPresignRequest` overload.
    *
    * The URL must use the `s3` scheme and contain a non-empty bucket and object key. User info, ports, query
    * parameters, and fragments are rejected. The signature duration must be positive. URL parsing and
    * validation occur eagerly and throw `IllegalArgumentException` when invalid; the delegated AWS operation
    * is deferred in `F`.
    */
  final def presignGetObject(
    s3Url: String,
    duration: FiniteDuration = 30.minutes): F[PresignedGetObjectRequest] = {
    val (bucket, key) = parseS3Url(s3Url)
    require(duration > Duration.Zero, s"Signature duration must be positive: $duration")
    presignGetObject(
      _.getObjectRequest(_.bucket(bucket).key(key): Unit)
        .signatureDuration(duration.toJava))
  }

  /*
   * private
   */
  private def parseS3Url(s3Url: String): (String, String) = {
    val uri = URI(s3Url)
    require("s3".equalsIgnoreCase(uri.getScheme), s"Expected an s3 URL, got: $s3Url")
    require(
      Option(uri.getUserInfo).isEmpty && uri.getPort < 0,
      s"S3 URL must not contain user info or a port: $s3Url")
    require(
      Option(uri.getQuery).isEmpty && Option(uri.getFragment).isEmpty,
      s"S3 URL must not contain a query or fragment: $s3Url")
    val bucket = Option(uri.getHost).getOrElse("")
    require(bucket.nonEmpty, s"S3 URL must contain a bucket: $s3Url")
    val key = uri.getPath.stripPrefix("/")
    require(key.nonEmpty, s"S3 URL must contain an object key: $s3Url")
    bucket -> key
  }
}

object SimpleStorageService:
  private val s3Name = "aws.s3"
  private val presignerName = "aws.s3.presigner"

  /** Create a managed S3 client and presigner resource using the supplied client-builder configuration. */
  def apply[F[_]](f: Endo[S3ClientBuilder])(using F: Sync[F]): Resource[F, SimpleStorageService[F]] =
    for {
      logger <- Resource.eval(Slf4jLogger.create[F])
      (s3, presigner) <- Resource.make(
        logger.info(s"initialize $presignerName/$s3Name") >>
          F.blocking {
            val s3 = f(S3Client.builder()).build()
            val conf = s3.serviceClientConfiguration()
            val pb = S3Presigner.builder()
              .region(conf.region())
              .credentialsProvider(conf.credentialsProvider())
            conf.endpointOverride().ifPresent(uri => pb.endpointOverride(uri): Unit)
            s3 -> pb.build()
          }) { (s3, presigner) =>
        shutdown(presignerName, logger)(presigner.close()) >>
          shutdown(s3Name, logger)(s3.close())
      }
    } yield new AwsS3[F](s3, presigner, logger)

  final private class AwsS3[F[_]](s3: S3Client, presigner: S3Presigner, logger: Logger[F])(using F: Sync[F])
      extends SimpleStorageService[F] {

    override def headObject(hor: HeadObjectRequest): F[HeadObjectResponse] =
      blockingF(s3.headObject(hor), hor.toString, logger)

    override def getObject(gor: GetObjectRequest): Resource[F, ResponseInputStream[GetObjectResponse]] =
      Resource.make(F.blocking(s3.getObject(gor)))(stream => F.blocking(stream.close()))

    override def putObject(body: RequestBody, por: PutObjectRequest): F[PutObjectResponse] =
      blockingF(s3.putObject(por, body), por.toString, logger)

    override def renameObject(ror: RenameObjectRequest): F[RenameObjectResponse] =
      blockingF(s3.renameObject(ror), ror.toString, logger)

    override def presignGetObject(gpr: GetObjectPresignRequest): F[PresignedGetObjectRequest] =
      blockingF(presigner.presignGetObject(gpr), gpr.toString, logger)
  }
end SimpleStorageService
