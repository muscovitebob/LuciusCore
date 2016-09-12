package com.dataintuitive.luciuscore.utils

object Statistics extends Serializable {

  /**
    * Calculate the median of a `Seq` of `Double`s
    */
  def median(l: Seq[Double]): Double = {

    require(l.size > 0, "The median of a zero-length list is not defined")

    val lsorted = l.sorted
    val length = l.size
    if (length % 2 == 0)
      (lsorted.apply(length / 2 - 1) + lsorted.apply(length / 2)) * 1.0 / 2
    else
      lsorted.apply(length / 2)
  }

}
