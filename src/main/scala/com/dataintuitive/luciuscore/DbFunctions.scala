package com.dataintuitive.luciuscore

import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.ZhangScoreFunctions.connectionScore

object DbFunctions {

  /**
    * Score an arbitrary number of rank vectors against a database entry.
    * @param x DbRow
    * @param queries Rank vector(s)
    * @return List of Option-wrapped tuples (DbRow, Seq(scores))
    */
  def queryDbRow(x: DbRow,
                     queries: Array[Double]*): Option[(DbRow, Seq[Double])] = {
    x.sampleAnnotations.r match {
      case Some(r) => Some(x, queries.map(query => connectionScore(r, query)))
      case _       => None
    }
  }

}
