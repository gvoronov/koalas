package koalas.test

import org.scalatest.FunSuite
import org.scalatest.Assertions._

// import scala.math.Numeric.{DoubleIsFractional, IntIsIntegral}
// import scala.math.Numeric.DoubleIsConflicted

// import koalas.datasets.bikesharing.BikeSharing
import koalas.datavalue._
import koalas.numericalops.NumericalOpsImps._
import koalas.series.Series

class SeriesSuite extends FunSuite {
  // implicit object dnum extends DoubleIsConflicted

  // implicit object inum extends Numeric[Int] with IntIsIntegral
  // implicit object NumericNV extends NumericNV

  test("series constructors") {
    val doubles: Series[Double] = Series[Double](1.0, 2.0, 3.0)
    val ints: Series[Int] = Series[Int](1, 2, 3)
    val numericalvalues: Series[NumericalValue] =
      Series(NumericalValue(1), NumericalValue(2), NumericalValue(3))
  }

  val doubles: Series[Double] = Series[Double](1.0, 2.0, 3.0)
  val ints: Series[Int] = Series[Int](1, 2, 3)
  val numericalvalues: Series[NumericalValue] =
    Series(NumericalValue(1), NumericalValue(2), NumericalValue(3))

  test("element wise operations") {
    info("doubles + doubles: " + (doubles :+ doubles).toString)
    info("ints + ints: " + (ints :+ ints).toString)
    info("numericalvalues + numericalvalues: " + (numericalvalues :+ numericalvalues).toString)

    info("doubles / doubles: " + (doubles :/ doubles).toString)
    // info("ints / ints: " + (ints / ints).toString)
    info("numericalvalues / numericalvalues: " + (numericalvalues :/ numericalvalues).toString)

    // info("doubles ** doubles: " + (doubles ** doubles).toString)
    // info("ints ** ints: " + (ints ** ints).toString)
    info("numericalvalues ** numericalvalues: " + (numericalvalues :** numericalvalues).toString)

    info("doubles :> 2: " + (doubles :> 2).toString)
    info("ints :> 2: " + (ints :> 2).toString)
    info("numericalvalues :> 2: " + (numericalvalues :> 2).toString)

  }

}
