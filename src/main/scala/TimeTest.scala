package timetest

import breeze.stats.distributions._
import datasets.bikesharing.BikeSharing
import datavalue._

object TimeTest {
  def main(args: Array[String]) {
    val g = Gaussian(1,1)
    val x = g.sample(100000).toIterable
    val y = x.map(NumericalValue(_))
    println("about to sum")
    println(x.sum)
    println(y.reduce((a, b) => a + b))

    val df = BikeSharing.getHourDF

    println("about to split and sum partitions")
    val (leftDF, rightDF) = df.partition(row => row[NumericalValue]("cnt") >= 162)

    val cntLeftSum = leftDF.select[NumericalValue]("cnt").reduce((a, b) => a + b)
    val cntRightSum = rightDF.select[NumericalValue]("cnt").reduce((a, b) => a + b)

    println(cntLeftSum, cntRightSum)
  }
}
