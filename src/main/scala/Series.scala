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

  private def binaryOpOnAnyToNumericalValue(
      that: Any, op: (NumericalValue, NumericalValue) => NumericalValue):
      Series[NumericalValue] = {

    that match {
      case series: Series[Any] => series(0) match {
        case element: NumericalValue => Series(
          values.zip(series.values).map(pair => op(
            pair._1.asInstanceOf[NumericalValue], pair._2.asInstanceOf[NumericalValue]))
        )
        case _ => throw new RuntimeException("")
      }
      case iterable: Iterable[Any] => iterable.last match {
        case element: NumericalValue => Series(
          values.zip(iterable).map(pair => op(
            pair._1.asInstanceOf[NumericalValue], pair._2.asInstanceOf[NumericalValue])).toVector
        )
        case _ => throw new RuntimeException("")
      }
      case value: NumericalValue => Series(values.map(y => op(y.asInstanceOf[NumericalValue], value)))
      case value: Double => Series(values.map(y => op(y.asInstanceOf[NumericalValue], NumericalValue(value))))
      case value: Int => Series(values.map(y => op(y.asInstanceOf[NumericalValue], NumericalValue(value))))
      case _ => throw new RuntimeException(
        "Series binary opertion attempted with non-numerical type")
    }
  }

  private def binaryOpOnAnyToBoolean(
      that: Any, op: (NumericalValue, NumericalValue) => Boolean):
      Series[Boolean] = {
    that match {
      case series: Series[Any] => series(0) match {
        case element: NumericalValue => Series(
          values.zip(series.values).map(pair => op(
            pair._1.asInstanceOf[NumericalValue], pair._2.asInstanceOf[NumericalValue]))
        )
        case _ => throw new RuntimeException("")
      }
      case iterable: Iterable[Any] => iterable.last match {
        case element: NumericalValue => Series(
          values.zip(iterable).map(pair => op(
            pair._1.asInstanceOf[NumericalValue], pair._2.asInstanceOf[NumericalValue])).toVector
        )
        case _ => throw new RuntimeException("")
      }
      case value: NumericalValue => Series(values.map(y => op(y.asInstanceOf[NumericalValue], value)))
      case value: Double => Series(values.map(y => op(y.asInstanceOf[NumericalValue], NumericalValue(value))))
      case value: Int => Series(values.map(y => op(y.asInstanceOf[NumericalValue], NumericalValue(value))))
      case _ => throw new RuntimeException(
        "Series binary opertion attempted with non-numerical type")
    }
  }

  def +(that: Any): Series[NumericalValue] = binaryOpOnAnyToNumericalValue(that, (a, b) => a + b)
  def -(that: Any): Series[NumericalValue] = binaryOpOnAnyToNumericalValue(that, (a, b) => a - b)
  def *(that: Any): Series[NumericalValue] = binaryOpOnAnyToNumericalValue(that, (a, b) => a * b)
  def /(that: Any): Series[NumericalValue] = binaryOpOnAnyToNumericalValue(that, (a, b) => a / b)
  def **(that: Any): Series[NumericalValue] = binaryOpOnAnyToNumericalValue(that, (a, b) => a ** b)

  def :>(that: Any): Series[Boolean] = binaryOpOnAnyToBoolean(that, (a, b) => a > b)
  def :>=(that: Any): Series[Boolean] = binaryOpOnAnyToBoolean(that, (a, b) => a >= b)
  def :<(that: Any): Series[Boolean] = binaryOpOnAnyToBoolean(that, (a, b) => a < b)
  def :<=(that: Any): Series[Boolean] = binaryOpOnAnyToBoolean(that, (a, b) => a <= b)
  def :==(that: Any): Series[Boolean] = binaryOpOnAnyToBoolean(that, (a, b) => a == b)

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
