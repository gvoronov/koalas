package timetest

import breeze.stats.distributions._

import datavalue._

object TimeTest {
  def main(args: Array[String]) {
    val g = Gaussian(1,1)
    val x = g.sample(100000).toIterable
    val y = x.map(NumericalValue(_))
    println("about to sum")
    println(x.sum)
    println(y.reduce((a, b) => a + b))
  }
}
