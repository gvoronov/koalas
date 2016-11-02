package koalas.series

import koalas.datavalue._

class Series[+T](val values: Vector[T]){
  def apply(index: Int): T = values(index)

  def map[R](f: (T) => R): Series[R] = Series[R](values.map(f))
  def reduce[R >: T](op: (R, R) => R): R = values.reduce(op)
  def filter(p: (T) => Boolean): Series[T] = Series[T](values.filter(p))
  def groupBy[R](f: (T) => R): Map[R, Series[T]] = values.groupBy(f).mapValues(Series[T](_))
  def partition(p: (T) => Boolean): (Series[T], Series[T]) = {
    val (left, right) = values.partition(p)
    (Series[T](left), Series[T](right))
  }

  def -(that: Series[NumericalValue]): Series[NumericalValue] =
    Series(values.zip(that.values).map(pair => pair._1.asInstanceOf[NumericalValue] - pair._2))

  def **(that: Series[NumericalValue]): Series[NumericalValue] =
    Series(values.zip(that.values).map(pair => pair._1.asInstanceOf[NumericalValue] ** pair._2))
  def **(that: NumericalValue): Series[NumericalValue] =
    Series(values.map(_.asInstanceOf[NumericalValue]**that))
  def **(that: Double): Series[NumericalValue] =
    Series(values.map(_.asInstanceOf[NumericalValue]**that))
  def **(that: Int): Series[NumericalValue] =
    Series(values.map(_.asInstanceOf[NumericalValue]**that))

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
