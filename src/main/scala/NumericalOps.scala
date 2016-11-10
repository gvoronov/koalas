package koalas.numericalops
// package koalas.datavalue

import math._

import koalas.datavalue._

object NumericalOps {
  implicit object NumericalValueOps extends FractionalNumericalValue
  implicit object DoubleOps extends FractionalDouble
  implicit object IntOps extends NumericInt
}

trait Exponentialable[T] {
  def pow(x: T, y: T): T
}

trait FractionalNumericalValue extends Fractional[NumericalValue] with Exponentialable[NumericalValue]{
  // Members declared in scala.math.Numeric
  def fromInt(x: Int): NumericalValue = NumericalValue(x)
  def minus(x: NumericalValue, y: NumericalValue): NumericalValue = x - y
  def negate(x: NumericalValue): NumericalValue = -x
  def plus(x: NumericalValue, y: NumericalValue): NumericalValue = x + y
  def times(x: NumericalValue, y: NumericalValue): NumericalValue = x * y
  def div(x: NumericalValue, y: NumericalValue): NumericalValue = x / y
  def pow(x: NumericalValue, y: NumericalValue): NumericalValue = x ** y
  def toDouble(x: NumericalValue): Double = x()
  def toFloat(x: NumericalValue): Float = x().toFloat
  def toInt(x: NumericalValue): Int = x().toInt
  def toLong(x: NumericalValue): Long = x().toLong

  // Members declared in scala.math.Ordering
  def compare(x: NumericalValue, y: NumericalValue): Int = x() compare y()
}

trait NumericInt extends Numeric[Int] {
  // Members declared in scala.math.Numeric
  def fromInt(x: Int): Int = x
  def minus(x: Int, y: Int): Int = x - y
  def negate(x: Int): Int = -x
  def plus(x: Int, y: Int): Int = x + y
  def times(x: Int, y: Int): Int = x * y
  def toDouble(x: Int): Double = x.toDouble
  def toFloat(x: Int): Float = x.toFloat
  def toInt(x: Int): Int = x.toInt
  def toLong(x: Int): Long = x.toLong

  // Members declared in scala.math.Ordering
  def compare(x: Int, y: Int): Int = x compare y
}

trait FractionalDouble extends Fractional[Double] {
  // Members declared in scala.math.Numeric
  def fromInt(x: Int): Double = x.toDouble
  def minus(x: Double, y: Double): Double = x - y
  def negate(x: Double): Double = -x
  def plus(x: Double, y: Double): Double = x + y
  def times(x: Double, y: Double): Double = x * y
  def div(x: Double, y: Double): Double = x / y
  def toDouble(x: Double): Double = x.toDouble
  def toFloat(x: Double): Float = x.toFloat
  def toInt(x: Double): Int = x.toInt
  def toLong(x: Double): Long = x.toLong

  // Members declared in scala.math.Ordering
  def compare(x: Double, y: Double): Int = x compare y
}
