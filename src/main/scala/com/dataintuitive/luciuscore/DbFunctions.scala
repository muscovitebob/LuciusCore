package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.ZhangScoreFunctions.connectionScore

object DbFunctions {
  /**
    * Score an arbitrary number of rank vectors against a database entry.
    * @param x DbRow
    * @param queries Rank vector(s)
    * @return Map, with DbRows' pwid as the key an a list of length(queries) as the value
    */
  def queryDbRow(x: DbRow,
                 queries: Array[Double]*): Map[Option[String], Seq[Option[Double]]] = {
    x.sampleAnnotations.r match {
      case Some(r) => Map(x.pwid ->
        queries.map(query => Option(connectionScore(r, query))))
      case _       => Map(None -> List(None))
    }
  }

}
