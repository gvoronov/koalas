package koalas.series

// import shapeless.TypeCase

import koalas.datavalue._

class Series[+T](val values: Vector[T]){
  def apply(index: Int): T = values(index)
  def apply(subset: Series[Boolean]): Series[T] =
    Series(values.zip(subset.values).filter(_._2).map(_._1))

  def map[R](f: (T) => R): Series[R] = Series[R](values.map(f))
  def reduce[R >: T](op: (R, R) => R): R = values.reduce(op)
  def filter(p: (T) => Boolean): Series[T] = Series[T](values.filter(p))
  def groupBy[R](f: (T) => R): Map[R, Series[T]] = values.groupBy(f).mapValues(Series[T](_))
  def partition(p: (T) => Boolean): (Series[T], Series[T]) = {
    val (left, right) = values.partition(p)
    (Series[T](left), Series[T](right))
  }

  private def binaryOpOnAny[D, R](that: Any, op: (D, D) => R): Series[R] = {
    that match {
      case series: Series[Any] => series(0) match {
        case element: NumericalValue => Series(
          values.zip(series.values).map(pair => op(
            pair._1.asInstanceOf[D], pair._2.asInstanceOf[D]))
        )
        case _ => throw new RuntimeException("")
      }
      case iterable: Iterable[Any] => iterable.last match {
        case element: NumericalValue => Series(
          values.zip(iterable).map(pair => op(
            pair._1.asInstanceOf[D], pair._2.asInstanceOf[D])).toVector
        )
        case _ => throw new RuntimeException("")
      }
      case value: NumericalValue => Series(values.map(y => op(y.asInstanceOf[D], value.asInstanceOf[D])))
      case value: Double => Series(values.map(y => op(y.asInstanceOf[D], NumericalValue(value).asInstanceOf[D])))
      case value: Int => Series(values.map(y => op(y.asInstanceOf[D], NumericalValue(value).asInstanceOf[D])))
      case _ => throw new RuntimeException(
        "Series binary opertion attempted with non-numerical type")
    }
  }

  /**
   * This method will check that self type matches that of right operatnd. Following this, it will
   * pattern match onto the the appropriate domaon type of the binary op.
   */
  // private def selectBinaryOpInType[A](that: Any):
  def +(that: Any): Series[NumericalValue] = binaryOpOnAny[NumericalValue, NumericalValue](that, (a, b) => a + b)
  def -(that: Any): Series[NumericalValue] = binaryOpOnAny[NumericalValue, NumericalValue](that, (a, b) => a - b)
  def *(that: Any): Series[NumericalValue] = binaryOpOnAny[NumericalValue, NumericalValue](that, (a, b) => a * b)
  def /(that: Any): Series[NumericalValue] = binaryOpOnAny[NumericalValue, NumericalValue](that, (a, b) => a / b)
  def **(that: Any): Series[NumericalValue] = binaryOpOnAny[NumericalValue, NumericalValue](that, (a, b) => a ** b)

  def :>(that: Any): Series[Boolean] = binaryOpOnAny[NumericalValue, Boolean](that, (a, b) => a > b)
  def :>=(that: Any): Series[Boolean] = binaryOpOnAny[NumericalValue, Boolean](that, (a, b) => a >= b)
  def :<(that: Any): Series[Boolean] = binaryOpOnAny[NumericalValue, Boolean](that, (a, b) => a < b)
  def :<=(that: Any): Series[Boolean] = binaryOpOnAny[NumericalValue, Boolean](that, (a, b) => a <= b)
  def :~=(that: Any): Series[Boolean] = binaryOpOnAny[NumericalValue, Boolean](that, (a, b) => a ~= b)
  def :~=(that: Any, precision: Double): Series[Boolean] = binaryOpOnAny[NumericalValue, Boolean](that, (a, b) => a ~= (b, precision))
  def :==(that: Any): Series[Boolean] = binaryOpOnAny[Any, Boolean](that, (a, b) => a equals b)
  def :!=(that: Any): Series[Boolean] = binaryOpOnAny[Any, Boolean](that, (a, b) => !(a equals b))

  lazy val length: Int = values.length
  lazy val sum: NumericalValue = values.asInstanceOf[Vector[NumericalValue]].reduce(_ + _)
  lazy val mean: NumericalValue = sum / length
  lazy val x1: NumericalValue = mean
  lazy val x2: NumericalValue =
    values.asInstanceOf[Vector[NumericalValue]].map(_**2.0).reduce(_ + _) / length
  lazy val variance: NumericalValue = x2 - x1**2.0
}

object Series{
  def apply[T](values: Vector[T]): Series[T] = new Series[T](values)
  def apply[T](values: Iterable[T]): Series[T] = new Series[T](values.toVector)
  // def apply[T](value: T*): Series[T]
}
