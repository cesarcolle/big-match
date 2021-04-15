package com.github.bigmatch.index


import com.github.bigmatch.common.UsualDateFormat
import com.github.bigmatch.{BigMatchTest, Tweet}
import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec

class IndextITest extends AnyFlatSpec with BigMatchTest {

  import spark.implicits._

  val date1 = UsualDateFormat.usualDateFormat.parseDateTime("2021-03-12-01")
  val date2 = UsualDateFormat.usualDateFormat.parseDateTime("2021-03-12-02")
  val date3 = UsualDateFormat.usualDateFormat.parseDateTime("2021-03-12-03")

  val dates = Seq(date1, date2, date3)
  val input: Seq[Tweet] = dates.map { date => Tweet("abc", date.getMillis, UsualDateFormat.usualDateFormat.print(date)) }

  "partitioning file" should " index by dates any case class" in {
    withTmpHadoopFolder { hdfsPath: Path =>
      BigMatchIndexer.index[Tweet](hdfsPath, spark.createDataset(input), spark.sparkContext.hadoopConfiguration)
      val files = fs.listStatus(hdfsPath)
      assertResult(dates.map(d => s"""date=${UsualDateFormat.usualDateFormat.print(d)}"""))(files.map(_.getPath.getName).toSeq.sorted)
    }
  }
}
