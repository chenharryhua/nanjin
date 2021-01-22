package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJTimestamp}

package object dstream {

  def pathBuilder(path: String)(ts: NJTimestamp): String =
    if (path.endsWith("/"))
      s"$path${ts.`Year=yyyy/Month=mm/Day=dd`(sydneyTime)}"
    else
      s"$path/${ts.`Year=yyyy/Month=mm/Day=dd`(sydneyTime)}"

}
