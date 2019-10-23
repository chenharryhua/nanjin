package com.github.chenharryhua.nanjin.database
import java.sql.{Date, Timestamp}

import doobie.util.Meta
import monocle.Iso

private[database] trait MetaDateTimeInstance {

  implicit def metaDoobieTimestamp[A](implicit iso: Iso[A, Timestamp]): Meta[A] =
    Meta[Timestamp].imap(iso.reverseGet)(iso.get)

  implicit def metaDoobieDate[A](implicit iso: Iso[A, Date]): Meta[A] =
    Meta[Date].imap(iso.reverseGet)(iso.get)
}
