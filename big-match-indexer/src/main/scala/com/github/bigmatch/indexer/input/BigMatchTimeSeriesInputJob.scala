package com.github.bigmatch.indexer.input

import com.github.bigmatch.index.{BigMatchIndexer, Indexable}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.joda.time.DateTime

abstract class BigMatchTimeSeriesInputJob[A  <: Indexable : Encoder] {

  def input(start: DateTime, end: DateTime) : Dataset[A]
  def outputPath : Path


  def run: Unit = {
    val start = System.getProperty("calcStart").toLong
    val end = System.getProperty("calcEnd").toLong
    val maxDocPerPartition = Option(System.getProperty("maxDoc")).map(_.toInt).getOrElse(1000)

    val master = System.getProperty("sparkMaster")

    val spark = SparkSession
      .builder()
      .master(master)
      .appName("big-match-indexer")
      .getOrCreate()

    BigMatchIndexer.index[A](
      baseOutputPath = outputPath,
      indexableDataset = input(new DateTime(start), new DateTime(end)),
      hadoopConf = spark.sparkContext.hadoopConfiguration,
      maxDocIndex = maxDocPerPartition
    )

  }

}
