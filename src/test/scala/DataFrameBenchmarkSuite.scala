package koalas.test

import org.scalatest.FunSuite
import org.scalameter._

import koalas.datasets.bikesharing.BikeSharing
import koalas.datavalue._
import koalas.dataframe.DataFrame

class DataFrameBenchmarkSuite extends FunSuite {
  val df = BikeSharing.loadHourDF

  test("partition bike-sharing dataframe and sum partioned column") {
    val partionTime = config(
      Key.exec.benchRuns -> 20,
      Key.verbose -> false
    ) withWarmer {
      new Warmer.Default
    } withMeasurer {
      new Measurer.IgnoringGC
    } measure {
      val (leftDF, rightDF) = df.partition(row => row[NumericalValue]("cnt") >= 162)
    }
    info("took " + partionTime.toString)
  }

  test("sum cnt column on full dataframe and two resulting partitioned dataframes") {
    val (leftDF, rightDF) = df.partition(row => row[NumericalValue]("cnt") >= 162)

    val allSumTime = config(
      Key.exec.benchRuns -> 20,
      Key.verbose -> false
    ) withWarmer {
      new Warmer.Default
    } withMeasurer {
      new Measurer.IgnoringGC
    } measure {
      val cntAlltSum = df.select[NumericalValue]("cnt").reduce((a, b) => a + b)
    }
    info("total sum took " + allSumTime.toString)

    val leftSumTime = config(
      Key.exec.benchRuns -> 20,
      Key.verbose -> false
    ) withWarmer {
      new Warmer.Default
    } withMeasurer {
      new Measurer.IgnoringGC
    } measure {
      val cntLeftSum = leftDF.select[NumericalValue]("cnt").reduce((a, b) => a + b)
    }
    info("left sum took " + leftSumTime.toString)

    val rightSumTime = config(
      Key.exec.benchRuns -> 20,
      Key.verbose -> false
    ) withWarmer {
      new Warmer.Default
    } withMeasurer {
      new Measurer.IgnoringGC
    } measure {
      val cntRightSum = rightDF.select[NumericalValue]("cnt").reduce((a, b) => a + b)
    }
    info("right sum took " + rightSumTime.toString)
  }
}
