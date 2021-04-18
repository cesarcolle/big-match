package com.github.bigmatch.search

import com.github.bigmatch.common.UsualDateFormat
import com.github.bigmatch.index.BigMatchIndexer
import com.github.bigmatch.{BigMatchTest, Tweet}
import org.apache.hadoop.fs.Path
import org.apache.lucene.index.Term
import org.apache.lucene.search.{MatchAllDocsQuery, Query, TermQuery}
import org.scalatest.flatspec.AnyFlatSpec

class BigMatchSearchTest extends AnyFlatSpec with BigMatchTest{
  import spark.implicits._

  val date = UsualDateFormat.usualDateFormat.parseDateTime("2021-03-12-01")
  val dateStr = UsualDateFormat.usualDateFormat.print(date)
  def tweet(txt: String, timestamp: Long = date.getMillis, dateStr : String = dateStr) = {
    Tweet(txt, timestamp, dateStr)
  }
  def test(input: Seq[Tweet], expected: Seq[Tweet], query: Query) ={
    withTmpHadoopFolder { tmp =>
      BigMatchIndexer.index(tmp, spark.createDataset(input), spark.sparkContext.hadoopConfiguration, 100)
      val path = input.map(t => s"$tmp/date=${t.date}").toSet
      val s = path.flatMap(p => BigMatchSearch.search[Tweet](query, spark, new Path(p), 10, Tweet.fromDocument))
        .reduce(_ union _)
      assertResult(expected.sortBy(_.text))(s.collect().toSeq.sortBy(_.text))
    }
  }

  "the search engine" should "read match_all doc after indexing them" in {
      import spark.implicits._
      val tweets = Seq(
        tweet("abc", date.getMillis, dateStr),
        tweet("acb", date.getMillis, dateStr)
      )
      test(tweets, tweets, new MatchAllDocsQuery())
    }

  "the search engine" should "read specific term query" in {
    val tweets = Seq(tweet("banana"), tweet("mango"), tweet("coconut"), tweet("coconut oil"))
    test(
      input = tweets,
      expected = Seq(tweet("coconut"), tweet("coconut oil")),
      query = new TermQuery(new Term("text", "coconut"))
    )
  }
}
