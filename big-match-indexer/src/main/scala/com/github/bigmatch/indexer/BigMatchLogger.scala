package com.github.bigmatch.indexer

import com.criteo.cuttle.Logger

object BigMatchLogger {
  implicit val logger = new Logger {
    def logMe(message: => String, level: String) =
      println(s"${java.time.Instant.now}\t${level}\t${message}")

    override def info(message: => String): Unit = logMe(message, "INFO")

    override def debug(message: => String): Unit = logMe(message, "DEBUG")

    override def warn(message: => String): Unit = logMe(message, "WARN")

    override def error(message: => String): Unit = logMe(message, "ERROR")

    override def trace(message: => String): Unit = ()
  }
}
