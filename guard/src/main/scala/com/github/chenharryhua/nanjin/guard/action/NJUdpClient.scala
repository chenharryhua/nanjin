package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.{Async, Resource}
import cats.implicits.toFunctorOps
import com.codahale.metrics.{Counter, Histogram, MetricRegistry}
import com.comcast.ip4s.{IpAddress, Port, SocketAddress}
import com.github.chenharryhua.nanjin.guard.config.{
  Category,
  CounterKind,
  HistogramKind,
  MetricID,
  MetricName
}
import fs2.Chunk
import fs2.io.net.{Datagram, DatagramSocket, Network}
import io.circe.syntax.EncoderOps
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

sealed trait UdpSocketWriter[F[_]] { def write(chunk: Chunk[Byte]): F[Unit] }

// udp works in fire and forget way so may not need timer to track performance
final class NJUdpClient[F[_]: Network](
  name: MetricName,
  metricRegistry: MetricRegistry,
  isCounting: Boolean,
  isHistogram: Boolean)(implicit F: Async[F]) {

  def histogram: NJUdpClient[F] = new NJUdpClient[F](name, metricRegistry, isCounting, isHistogram = true)
  def counted: NJUdpClient[F]   = new NJUdpClient[F](name, metricRegistry, isCounting = true, isHistogram)

  private class Writer(socket: DatagramSocket[F], remote: SocketAddress[IpAddress]) {
    private val histogramId: MetricID =
      MetricID(name, Category.Histogram(HistogramKind.UdpHistogram, StandardUnit.BYTES))
    private val counterId: MetricID =
      MetricID(name, Category.Counter(CounterKind.UdpCounter))

    val writer: UdpSocketWriter[F] =
      (isHistogram, isCounting) match {
        case (true, true) =>
          new UdpSocketWriter[F] {
            private lazy val histogram: Histogram = metricRegistry.histogram(histogramId.asJson.noSpaces)
            private lazy val counter: Counter     = metricRegistry.counter(counterId.asJson.noSpaces)

            override def write(chunk: Chunk[Byte]): F[Unit] = socket.write(Datagram(remote, chunk)).map { _ =>
              histogram.update(chunk.size)
              counter.inc(1)
            }
          }
        case (true, false) =>
          new UdpSocketWriter[F] {
            private lazy val histogram: Histogram = metricRegistry.histogram(histogramId.asJson.noSpaces)

            override def write(chunk: Chunk[Byte]): F[Unit] = socket.write(Datagram(remote, chunk)).map { _ =>
              histogram.update(chunk.size)
            }
          }
        case (false, true) =>
          new UdpSocketWriter[F] {
            private lazy val counter: Counter = metricRegistry.counter(counterId.asJson.noSpaces)

            override def write(chunk: Chunk[Byte]): F[Unit] = socket.write(Datagram(remote, chunk)).map { _ =>
              counter.inc(1)
            }
          }
        case (false, false) =>
          new UdpSocketWriter[F] {
            override def write(chunk: Chunk[Byte]): F[Unit] = socket.write(Datagram(remote, chunk))
          }
      }
  }

  def socket(outboundIp: IpAddress, outboundPort: Port): Resource[F, UdpSocketWriter[F]] =
    Network[F]
      .openDatagramSocket()
      .map((socket: DatagramSocket[F]) => new Writer(socket, SocketAddress(outboundIp, outboundPort)).writer)
}
