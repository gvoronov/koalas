package koalas.test

import org.scalatest.FunSuite

import koalas.datasets.bikesharing.BikeSharing
import koalas.datavalue._

class DataFrameSuite extends FunSuite {
  val df = BikeSharing.getHourDF

  test("get numerical value series and sum it") {
    val atempSum1 = df.select[NumericalValue]("atemp").reduce((a, b) => a + b)
    assert(atempSum1 ~= 8268.4955)

    val atempSum2 = df.select[NumericalValue]("atemp").sum
    assert(atempSum2 ~= 8268.4955)

    val atempSum3 = df.sum("atemp")
    assert(atempSum3 ~= 8268.4955)
  }

  test("get numerical value series and get its mean") {
    val atempMeanTuple: Tuple2[NumericalValue, Int] = df
      .select[NumericalValue]("atemp")
      .map(atemp => (atemp, 1))
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    val atempMean1: NumericalValue = atempMeanTuple._1 / atempMeanTuple._2
    assert(atempMean1 ~= 0.47577510213476037)

    val atempMean2 = df.select[NumericalValue]("atemp").mean
    assert(atempMean2 ~= 0.47577510213476037)
  }

  test("partition dataframe and sum partioned column") {
    val (leftDF, rightDF) = df.partition(row => row[NumericalValue]("cnt") >= 162)

    val cntLeftSum = leftDF.select[NumericalValue]("cnt").reduce((a, b) => a + b)
    val cntRightSum = rightDF.select[NumericalValue]("cnt").reduce((a, b) => a + b)

    assert(cntRightSum == 561079)
    assert(cntLeftSum == 2731600)
  }
}
