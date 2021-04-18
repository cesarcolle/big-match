package com.github.bigmatch.indexer.scheduler

import com.criteo.cuttle.timeseries.{CuttleProject, hourly}
import com.criteo.cuttle.{DatabaseConfig, Job}
import com.github.bigmatch.indexer.BigMatchLogger

import com.criteo.cuttle._
import com.criteo.cuttle.platforms.local._
import com.criteo.cuttle.timeseries._

import java.time.ZoneOffset.UTC
import java.time.{Instant, LocalDate, ZoneId}

object BigMatchIndexerScheduler {

  import BigMatchLogger._

  def sparkSubmit(sparkHome: String,
                  jobName: String,
                  jobClass: String,
                  masterUrl: String,
                  jarPath: String,
                  jobArgs: String,
                  conf: Seq[(String, String)] = Nil) = {
    s"""
       |${sparkHome}/bin/spark-submit \\
       |  --class $jobClass \\
       |  --name $jobName \\
       |  --master  $masterUrl \\
       |  $jarPath \\
       |  $jobArgs
       |""".stripMargin

  }

  val dateTime = java.time.format.DateTimeFormatter.ofPattern("MM/dd/yyyy:HH").withZone(ZoneId.systemDefault())


  def main(args: Array[String]): Unit = {

    val sparkHome = System.getenv().get("SPARK_HOME")
    val sparkJarPath = "target/scala-2.12/big-match.jar"
    val masterUrl = System.getenv().getOrDefault("SPARK_URL", "local")



    implicit val l = java.util.logging.Logger.getLogger(this.getClass.getName)
    val start: Instant = LocalDate.now.atStartOfDay.toInstant(UTC)

    val indexer = Job("big-match-indexer", hourly(start), "indexer") { implicit e =>
      e.streams.info("Run Cohort Job")
      val indexerClassName = "com.github.indexer.BigMatchIndexerJob"
      exec"${
        sparkSubmit(sparkHome,
          "BigMatchIndexer",
          indexerClassName,
          masterUrl,
          sparkJarPath, s"-Dspark=$masterUrl -DcalcStart=${e.context.start.toEpochMilli} -DcalcEnd=${e.context.end.toEpochMilli}"
        )
      }"()
    }

    CuttleProject("BIG-MATCH", version = "1", env = ("dev", false)) {
      indexer
    }(logger = logger).start(databaseConfig = DatabaseConfig.fromEnv)
  }
}
