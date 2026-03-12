package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.common.Opaque
import io.circe.{Decoder, Encoder, Json}

import java.time.{Duration, ZoneId}
import java.util.UUID

opaque type Task = String
object Task:
  def apply(value: String): Task = value
  extension (t: Task) inline def value: String = t

  given Show[Task] = _.value
  given Encoder[Task] = Opaque.lift[Task, String, Encoder]
  given Decoder[Task] = Opaque.lift[Task, String, Decoder]
end Task

opaque type Service = String
object Service:
  def apply(value: String): Service = value
  extension (s: Service) inline def value: String = s

  given Show[Service] = _.value
  given Encoder[Service] = Opaque.lift[Service, String, Encoder]
  given Decoder[Service] = Opaque.lift[Service, String, Decoder]
end Service

opaque type ServiceId = UUID
object ServiceId:
  def apply(value: UUID): ServiceId = value
  extension (s: ServiceId) inline def value: UUID = s

  given Show[ServiceId] = _.value.show
  given Encoder[ServiceId] = Opaque.lift[ServiceId, UUID, Encoder]
  given Decoder[ServiceId] = Opaque.lift[ServiceId, UUID, Decoder]
end ServiceId

opaque type Homepage = String
object Homepage:
  def apply(value: String): Homepage = value
  extension (h: Homepage) inline def value: String = h

  given Encoder[Homepage] = Opaque.lift[Homepage, String, Encoder]
  given Decoder[Homepage] = Opaque.lift[Homepage, String, Decoder]
end Homepage

opaque type Port = Int
object Port:
  def apply(value: Int): Port = value
  extension (p: Port) inline def value: Int = p

  given Show[Port] = _.value.toString
  given Encoder[Port] = Opaque.lift[Port, Int, Encoder]
  given Decoder[Port] = Opaque.lift[Port, Int, Decoder]
end Port

opaque type Brief = Json
object Brief:
  def apply(value: Json): Brief = value
  extension (b: Brief) inline def value: Json = b

  given Show[Brief] = _.value.spaces2
  given Encoder[Brief] = Opaque.lift[Brief, Json, Encoder]
  given Decoder[Brief] = Opaque.lift[Brief, Json, Decoder]
end Brief

opaque type TimeZone = ZoneId
object TimeZone:
  def apply(zoneId: ZoneId): TimeZone = zoneId
  extension (tz: TimeZone) inline def value: ZoneId = tz

  given Show[TimeZone] = Show.fromToString
  given Encoder[TimeZone] = Opaque.lift[TimeZone, ZoneId, Encoder]
  given Decoder[TimeZone] = Opaque.lift[TimeZone, ZoneId, Decoder]
end TimeZone

opaque type UpTime = Duration
object UpTime:
  def apply(duration: Duration): UpTime = duration
  extension (upTime: UpTime) inline def value: Duration = upTime

  given Show[UpTime] = defaultFormatter.format(_)
  given Encoder[UpTime] = Opaque.lift[UpTime, Duration, Encoder]
  given Decoder[UpTime] = Opaque.lift[UpTime, Duration, Decoder]
end UpTime
