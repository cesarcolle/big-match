# The Big Match (incubator)
A very slow but cheap and free timeout guarantee search text engine.
Powered by Lucene query

## Introduction
Someday you'll need a slow but cheap and timeout free text search engine. Elasticsearch is a best tool of the market to go fast. But it has a cost.
Big-Match is slow but cheap.

## High level architecture

![sophia-definition](big-match.png)

## Example

### Indexer

```scala
import com.github.bigmatch.indexer.input.BigMatchTimeSeriesInputJob

case class Tweet(text: String, timestamp: Long, date: String) extends Indexable
class TweetScheduler extends BigMatchTimeSeriesInputJob[Tweet] {
  def input(start: DateTime, end: DateTime) : Dataset[Tweet] =
    spark.read.parquet(s"/user/big-match/${start}.csv", s"/user/big-match/${end}.csv")
      .as[Tweet]
      
  def output: Path = new Path("/user/big-match/output")
}

```

## Description

### Stack:
the core of big match relies on:
- Apache Spark
- Apache Solr 

