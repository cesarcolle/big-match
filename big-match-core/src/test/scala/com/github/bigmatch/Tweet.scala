package com.github.bigmatch

import com.github.bigmatch.common.UsualDateFormat
import com.github.bigmatch.index.Indexable
import org.apache.lucene.document.{Document, Field, TextField}

object Tweet {
  def fromDocument(doc: Document) = {
    val timestamp = doc.get("timestamp").toLong
    Tweet(
      text = doc.get("text"),
      timestamp = timestamp,
      date = UsualDateFormat.usualDateFormat.print(timestamp)
    )
  }
}

case class Tweet(text: String, timestamp: Long, date: String) extends Indexable {
  override def toDocument: Document = {
    val d = new Document()
    d.add(new Field("text", text, TextField.TYPE_STORED))
    d.add(new Field("timestamp", timestamp.toString, TextField.TYPE_STORED))
    d
  }
}
