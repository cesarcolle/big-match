package com.github.bigmatch.index


import com.github.bigmatch.common.HadoopConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.solr.store.hdfs.HdfsDirectory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoder}

import java.util.UUID
import scala.annotation.tailrec
import scala.collection.JavaConverters._

object BigMatchIndexer extends Logging {

  def analyzer = {
    val config = new IndexWriterConfig(new StandardAnalyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
    config
  }


  def index[A <: Indexable : Encoder](
                                       baseOutputPath: Path,
                                       indexableDataset: Dataset[A],
                                       hadoopConf: Configuration,
                                       maxDocIndex: Int = 1000): Unit = {
    val datasetRepartition: Dataset[A] = indexableDataset
      .repartition(col("date"))

    val serHadoopConfiguration = new HadoopConf(hadoopConf)

    def getFiles(path: Path) = FileSystem.get(serHadoopConfiguration.get()).listStatus(path)
    def fileExists(path: Path) = FileSystem.get(serHadoopConfiguration.get()).exists(path)
    def mkDirs(path: Path) = FileSystem.get(serHadoopConfiguration.get()).mkdirs(path)

    datasetRepartition.foreachPartition { partition: Iterator[A] =>
      // TODO: possible to just use the iterator.
      val p = partition.toSeq
      // At this stage every row should stay on the same partition
      if (p.nonEmpty) {
        val baseIndexPath = new Path(getDirectory(baseOutputPath.toString, p.head.date))
        val id = if (fileExists(baseIndexPath)) {
          val indexes = getFiles(baseIndexPath)
          findOrCreateIndex(indexes, baseIndexPath, serHadoopConfiguration, maxDocIndex, p.length)
        } else {
          mkDirs(baseIndexPath)
          createNewIndex(baseIndexPath, serHadoopConfiguration)
        }
        id.addDocuments(p.map(_.toDocument.getFields()).asJava)
        id.close()
      }
      ()
    }
  }

  private def findOrCreateIndex(existingIndex: IndexedSeq[FileStatus],
                                basePath: Path,
                                hadoopConf: HadoopConf,
                                maxDocIndex: Int,
                                sizePartition: Int) = {
    @tailrec
    def findOrCreateIndexRecc(id: Int): IndexWriter = {
      if (id < existingIndex.size) {
        val currentIndex = existingIndex(id)
        val indexDirectory = new HdfsDirectory(currentIndex.getPath, hadoopConf.get())
        val index = new IndexWriter(indexDirectory, analyzer)
        if (index.getDocStats.numDocs + sizePartition < maxDocIndex) {
          index
        } else {
          findOrCreateIndexRecc(id + 1)
        }
      } else {
        createNewIndex(basePath, hadoopConf)
      }
    }
    findOrCreateIndexRecc(0)
  }

  def createNewIndex(basePath: Path, hadoopConf: HadoopConf) = {
    val indexName = s"${UUID.randomUUID().toString}"
    val newIndexPath = new Path(basePath, indexName)

    logInfo("creating new index:" + newIndexPath)

    val indexDirectory = new HdfsDirectory(newIndexPath, hadoopConf.get())
    new IndexWriter(indexDirectory, analyzer)
  }

  def getDirectory(basePath: String, date: String) = s"""${basePath}/date=${date}"""




}
