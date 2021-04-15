package com.github.bigmatch.search

import com.github.bigmatch.common.UsualDateFormat
import com.github.bigmatch.index.BigMatchIndexer
import com.github.bigmatch.{BigMatchTest, Tweet}
import org.apache.hadoop.fs.Path
import org.apache.lucene.search.{MatchAllDocsQuery, Query}
import org.scalatest.flatspec.AnyFlatSpec

class BigMatchSearchTest extends AnyFlatSpec with BigMatchTest{

  val date = UsualDateFormat.usualDateFormat.parseDateTime("2021-03-12-01")
  val dateStr = UsualDateFormat.usualDateFormat.print(date)

  "the search engine" should "read match_all doc after indexing them" in {
    import spark.implicits._

    val tweets = Seq(
      Tweet("abc", date.getMillis, dateStr),
      Tweet("acb", date.getMillis, dateStr),
      Tweet("bac", date.getMillis, dateStr),
      Tweet("cab", date.getMillis, dateStr),
      Tweet("cba", date.getMillis, dateStr),
    )

    withTmpHadoopFolder { tmp =>
      BigMatchIndexer.index[Tweet](tmp, spark.createDataset[Tweet](tweets), spark.sparkContext.hadoopConfiguration)
      val input = new Path(s"$tmp/date=$dateStr")
      val matchAll: Query = new MatchAllDocsQuery()
      val s = BigMatchSearch.search(matchAll, spark, input, 10, Tweet.fromDocument)
      assertResult(tweets.sortBy(_.text))(s.get.collect().toSeq.sortBy(_.text))
    }


  }

}
