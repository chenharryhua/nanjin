package com.github.chenharryhua.nanjin.common

import java.time.{LocalDate, LocalDateTime}

package object time {
  def year_month_day(ld: LocalDate): String =
    f"Year=${ld.getYear}%4d/Month=${ld.getMonthValue}%02d/Day=${ld.getDayOfMonth}%02d"

  def year_month_day_hour(ldt: LocalDateTime): String =
    year_month_day(ldt.toLocalDate) + f"/Hour=${ldt.getHour}%02d"

  def year_month_day_hour_minute(ldt: LocalDateTime): String =
    year_month_day_hour(ldt) + f"/Minute=${ldt.getMinute}%02d"
}
