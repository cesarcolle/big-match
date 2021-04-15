package com.github.bigmatch.common

import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat

object UsualDateFormat {

  val usualDateFormat = DateTimeFormat.forPattern("YYYY-MM-dd-HH").withZone(DateTimeZone.UTC)


}
