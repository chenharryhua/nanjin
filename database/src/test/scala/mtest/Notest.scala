package mtest

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import com.github.chenharryhua.nanjin.database._
import doobie.util.Meta
 
class Notest {
  val instant                            = Meta[Instant]
  val date                               = Meta[Date]
  val timestamp                          = Meta[Timestamp]
  val localdate                          = Meta[LocalDate]
  implicit val zoneId                    = ZoneId.systemDefault()
  val localdatetime: Meta[LocalDateTime] = Meta[LocalDateTime]
}
 