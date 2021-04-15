package com.github.bigmatch.index

import org.apache.lucene.document.Document
import org.joda.time.format.DateTimeFormat


trait Indexable {
  val timestamp : Long
  val date : String
  def toDocument: Document
}


