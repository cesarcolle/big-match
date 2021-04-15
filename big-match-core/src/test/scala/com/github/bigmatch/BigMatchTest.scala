package com.github.bigmatch

import java.nio.file.{Files, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

object BigMatchTest {

  private def builderTest: SparkSession.Builder = {
    SparkSession.builder()
      .master("local[*]")
      .appName("test")
      .config("spark.ui.enabled", false)
      .config("spark.sql.shuffle.partitions", 4)
      .config("spark.sql.session.timeZone", "UTC")
  }

  private var currentSession: Option[SparkSession] = None
  private var checkpointDir: Option[Path] = None

  def sparkConf: Map[String, Any] = Map()

  def getOrCreateSparkSession = {
    synchronized {
      if (currentSession.isEmpty) {
        val builder = builderTest
        for ((key, value) <- sparkConf) {
          builder.config(key, value.toString)
        }
        builder.config("spark.kryo.registrator", classOf[Configuration].getName)
        currentSession = Some(builder.getOrCreate())
        checkpointDir = Some(Files.createTempDirectory("spark-unit-test-checkpoint-"))
        currentSession.get.sparkContext.setCheckpointDir(checkpointDir.get.toString)
      }
      currentSession.get
    }
  }
}

trait BigMatchTest {
  val spark = BigMatchTest.getOrCreateSparkSession
  val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)

  def withTmpHadoopFolder(test : org.apache.hadoop.fs.Path => Unit) =
    test(new org.apache.hadoop.fs.Path(Files.createTempDirectory("").toString))


}
