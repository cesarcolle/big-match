package com.github.bigmatch.search

import com.github.bigmatch.common.UsualDateFormat
import com.github.bigmatch.index.Indexable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.lucene.document.Document
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.{IndexSearcher, Query, ScoreDoc}
import org.apache.solr.store.hdfs.HdfsDirectory
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.joda.time.{DateTime, Period}

import scala.annotation.tailrec

object BigMatchSearch {

  def searchTimeline[A <: Indexable : Encoder](query: Query,
                                       spark: SparkSession,
                                       baseInputPath: Path,
                                       batch: Int,
                                       calcEnd: DateTime,
                                       calcStart: DateTime,
                                       transform: Document => A
                                      ): Option[Dataset[A]] = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val calcStartStr = UsualDateFormat.usualDateFormat.print(calcStart)
    val calcEndStr = UsualDateFormat.usualDateFormat.print(calcEnd)

    val paths = fs.listStatus(baseInputPath).filter{ p =>
      val datePartition = p.getPath.getName

      datePartition <= calcEndStr && datePartition >= calcStartStr
    }
    if (paths.isEmpty){
      None
    } else {
      paths.flatMap(partiton => search[A](query, spark, partiton.getPath, batch, transform))
        .reduceOption(_ union _)
    }

  }

  def search[A <: Indexable : Encoder](query: Query,
                                       spark: SparkSession,
                                       pathToSearch: Path,
                                       batch: Int,
                                       transform: Document => A): Option[Dataset[A]] = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    val fs = FileSystem.get(hadoopConf)
    val files = fs.listStatus(pathToSearch)
    val matchedDocs = files.flatMap { indexPath =>
      if (indexPath.isDirectory) {
        val hdfsDir = new HdfsDirectory(indexPath.getPath, hadoopConf)
        val indexSearcher =  new IndexSearcher(DirectoryReader.open(hdfsDir))
        Some(luceneDocToRdd(indexSearcher, spark, query, batch, transform))
      } else {
        None
      }
    }
    matchedDocs.reduceOption(_ union _)
  }

  private def luceneDocToRdd[A : Encoder](indexSearch: IndexSearcher,
                                spark: SparkSession,
                                query: Query,
                                batch: Int,
                                transform: Document => A) = {
    import spark.implicits._
    @tailrec
    def luceneDocToRddRecc(df: Dataset[A], from: ScoreDoc): Dataset[A] = {
      val docs = indexSearch.searchAfter(from, query, batch)
      if (docs.scoreDocs.length == 0L){
        df
      } else {
        val union = df.union(spark.createDataset[A](docs.scoreDocs.map(scoreDoc => transform(indexSearch.doc(scoreDoc.doc)))))
        luceneDocToRddRecc(union, docs.scoreDocs.last)
      }
    }
    luceneDocToRddRecc(spark.emptyDataset[A], null)
  }

  def load[A <: Indexable : Encoder](spark: SparkSession,
                                     baseInputPath: Path,
                                     calcStart: DateTime,
                                     calcEnd: DateTime): Dataset[A] = {
    val calcStartStr = UsualDateFormat.usualDateFormat.print(calcStart)
    val calcEndStr = UsualDateFormat.usualDateFormat.print(calcEnd)

    val partitions = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      .listStatus(baseInputPath)
      .flatMap(_.getPath
        .toString
        .split("=")
        .lastOption
        .filter((partition => partition > calcStartStr && partition <= calcEndStr))
      )
    if (partitions.isEmpty) {
      spark.emptyDataset[A]
    } else {
      partitions.map(spark.read.parquet(_).as[A]).reduce(_.union(_))
    }
  }

}
