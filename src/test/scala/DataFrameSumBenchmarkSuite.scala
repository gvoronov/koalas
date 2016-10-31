package test

import org.scalatest.FunSuite
import org.scalameter._

import koalas.datasets.bikesharing.BikeSharing
import koalas.datavalue._
import koalas.dataframe.DataFrame

class DataFrameSumBenchmarkSuite extends FunSuite {
  val df = BikeSharing.getHourDF

  test("series sum") {
    val partionTime = config(
      Key.exec.benchRuns -> 20,
      Key.verbose -> false
    ) withWarmer {
      new Warmer.Default
    } withMeasurer {
      new Measurer.IgnoringGC
    } measure {
      val atempSum = df.select[NumericalValue]("atemp").sum
    }
    info("took " + partionTime.toString)
  }

  test("inplace sum") {
    val partionTime = config(
      Key.exec.benchRuns -> 20,
      Key.verbose -> false
    ) withWarmer {
      new Warmer.Default
    } withMeasurer {
      new Measurer.IgnoringGC
    } measure {
      val atempSum = df.sum("atemp")
    }
    info("took " + partionTime.toString)
  }

  test("vector sum") {
    val partionTime = config(
      Key.exec.benchRuns -> 20,
      Key.verbose -> false
    ) withWarmer {
      new Warmer.Default
    } withMeasurer {
      new Measurer.IgnoringGC
    } measure {
      val atempSum = df.select[NumericalValue]("atemp").map(_()).values.sum
    }
    info("took " + partionTime.toString)
  }
}
