package koalas.series

import scala.util.Random
import scala.collection.mutable

import koalas.datavalue._
import koalas.numericalops._
import koalas.numericalops.NumericalOps._

class Series[+T](val values: Vector[T]){
  private val mySummary: mutable.Map[String, Any] = mutable.Map.empty
  lazy val length: Int = values.length
  lazy val isEmpty: Boolean = values.isEmpty

  def apply(index: Int): T = values(index)
  def apply(index: Iterable[Int]) = Series[T](index.toVector.map(values(_)))
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
  def distinct: Series[T] = Series[T](values.distinct)
  def sample(numSamples: Int, withReplacement: Boolean): Series[T] = {
    if (withReplacement)
      Series[T](Vector.fill(numSamples)(Random.nextInt(length)).map(values(_)))
    else
      Series[T](Random.shuffle((0 until length).toVector).take(numSamples).map(values(_)))
  }
  def +[A >: T](that: A): Series[A] = Series(values :+ that)
  def +[A >: T](that: Series[A]): Series[A] = Series(values ++ that.values)

  override def toString: String = values.toString

  lazy val last: T = values.last

  private def binaryOpOnAny[D, R](that: Any, op: (D, D) => R): Series[R] = {
    if (isEmpty)
      return Series.empty[R]

    that match {
      case series: Series[Any] => {
        series.last match {
          case _: NumericalValue => assert(last.isInstanceOf[NumericalValue])
          case _: Double => assert(last.isInstanceOf[Double])
          case _: Int => assert(last.isInstanceOf[Int])
        }
        Series(values.zip(series.values).map(pair => op(pair._1.asInstanceOf[D], pair._2.asInstanceOf[D])))
      }
      case iterable: Iterable[Any] => {
        iterable.last match {
          case _: NumericalValue => assert(last.isInstanceOf[NumericalValue])
          case _: Double => assert(last.isInstanceOf[Double])
          case _: Int => assert(last.isInstanceOf[Int])
        }
        Series(values.zip(iterable).map(pair => op(pair._1.asInstanceOf[D], pair._2.asInstanceOf[D])).toVector)
      }
      case value: NumericalValue => last match {
        case _: NumericalValue => Series(values.map(y => op(y.asInstanceOf[D], value.asInstanceOf[D])))
        case _: Double => Series(values.map(y => op(y.asInstanceOf[D], value().asInstanceOf[D])))
        case _: Int => Series(values.map(y => op(y.asInstanceOf[D], value().asInstanceOf[D])))
        case _ => throw new RuntimeException("")
      }
      case value: Double => last match {
        case _: NumericalValue => Series(values.map(y => op(y.asInstanceOf[D], NumericalValue(value).asInstanceOf[D])))
        case _: Double => Series(values.map(y => op(y.asInstanceOf[D], value.asInstanceOf[D])))
        case _: Int => Series(values.map(y => op(y.asInstanceOf[D], value.toInt.asInstanceOf[D])))
        case _ => throw new RuntimeException("")
      }
      case value: Int => last match {
        case _: NumericalValue => Series(values.map(y => op(y.asInstanceOf[D], NumericalValue(value).asInstanceOf[D])))
        case _: Double => Series(values.map(y => op(y.asInstanceOf[D], value.toDouble.asInstanceOf[D])))
        case _: Int => Series(values.map(y => op(y.asInstanceOf[D], value.asInstanceOf[D])))
        case _ => throw new RuntimeException("")
      }
      case _ => throw new RuntimeException(
        "Series binary opertion attempted with non-numerical type")
    }
  }

  /**
   * Element-wise addition
   * @param that
   * @param num
   * @tparam B
   */
  def :+[B >: T](that: Any)(implicit num: Numeric[B]): Series[T] =
    binaryOpOnAny[B, B](that, num.plus).asInstanceOf[Series[T]]
  def :-[B >: T](that: Any)(implicit num: Numeric[B]): Series[T] =
    binaryOpOnAny[B, B](that, num.minus).asInstanceOf[Series[T]]
  def :*[B >: T](that: Any)(implicit num: Numeric[B]): Series[T] =
    binaryOpOnAny[B, B](that, num.times).asInstanceOf[Series[T]]
  def :/[B >: T](that: Any)(implicit num: Fractional[B]): Series[T] =
    binaryOpOnAny[B, B](that, num.div).asInstanceOf[Series[T]]
  def :**[B >: T](that: Any)(implicit num: Numeric[B] with Exponentialable[B]): Series[T] =
    binaryOpOnAny[B, B](that, num.pow).asInstanceOf[Series[T]]

  def :>[B >: T](that: Any)(implicit num: Ordering[B]): Series[Boolean] =
    binaryOpOnAny[B, Boolean](that, num.gt).asInstanceOf[Series[Boolean]]
  def :>=[B >: T](that: Any)(implicit num: Ordering[B]): Series[Boolean] =
    binaryOpOnAny[B, Boolean](that, num.gteq).asInstanceOf[Series[Boolean]]
  def :<[B >: T](that: Any)(implicit num: Ordering[B]): Series[Boolean] =
    binaryOpOnAny[B, Boolean](that, num.lt).asInstanceOf[Series[Boolean]]
  def :<=[B >: T](that: Any)(implicit num: Ordering[B]): Series[Boolean] =
    binaryOpOnAny[B, Boolean](that, num.lteq).asInstanceOf[Series[Boolean]]
  def :==[B >: T](that: Any)(implicit num: Ordering[B]): Series[Boolean] =
    binaryOpOnAny[B, Boolean](that, num.equiv).asInstanceOf[Series[Boolean]]
  def :!=[B >: T](that: Any)(implicit num: Ordering[B]): Series[Boolean] =
    binaryOpOnAny[B, Boolean](that, (a, b) => !num.equiv(a, b)).asInstanceOf[Series[Boolean]]

  def :~=(that: Any): Series[Boolean] = binaryOpOnAny[NumericalValue, Boolean](that, (a, b) => a ~= b)
  def :~=(that: Any, precision: Double): Series[Boolean] = binaryOpOnAny[NumericalValue, Boolean](that, (a, b) => a ~= (b, precision))

  def :&&(that: Any): Series[Boolean] = binaryOpOnAny[Boolean, Boolean](that, (a, b) => a && b)
  def :||(that: Any): Series[Boolean] = binaryOpOnAny[Boolean, Boolean](that, (a, b) => a || b)
  def :!(): Series[Boolean] = {
    if (isEmpty)
      Series.empty[Boolean]
    else
      Series[Boolean](values.map(!_.asInstanceOf[Boolean]))
  }
  def sorted[B >: T](implicit num: Ordering[B]): Series[T] = Series(values.sorted(num))

  def sum[A >: T](implicit num: Numeric[A]): A = mySummary.getOrElseUpdate(
    "sum", values.fold(num.zero)(num.plus)).asInstanceOf[A]
  def mean[A >: T](implicit num: Fractional[A]): A = mySummary.getOrElseUpdate(
    "mean", num.div(sum(num), num.fromInt(length))).asInstanceOf[A]
  def moment1[A >: T](implicit num: Fractional[A]): A = mySummary.getOrElseUpdate(
    "moment1", mean(num)).asInstanceOf[A]
  def moment2[A >: T](implicit num: Fractional[A] with Exponentialable[A]): A =
    mySummary.getOrElseUpdate(
      "moment2", num.div(map(num.pow(_, num.fromInt(2))).reduce(num.plus), num.fromInt(length))
    ).asInstanceOf[A]
  def variance[A >: T](implicit num: Fractional[A] with Exponentialable[A]): A =
    mySummary.getOrElseUpdate(
      "variance", num.minus(moment2(num), num.pow(moment1(num), num.fromInt(2)))
    ).asInstanceOf[A]
  // Figure out what to do with empty series
  def min[A >: T](implicit num: Numeric[A]): A = mySummary.getOrElseUpdate(
    "min", values.reduce(num.min)).asInstanceOf[A]
  def max[A >: T](implicit num: Numeric[A]): A = mySummary.getOrElseUpdate(
    "max", values.reduce(num.max)).asInstanceOf[A]
}

object Series{
  def apply[T](values: Vector[T]): Series[T] = new Series[T](values)
  def apply[T](values: Iterable[T]): Series[T] = new Series[T](values.toVector)
  def apply[T](values: T*): Series[T] = new Series[T](values.toVector)

  def fill[T](n: Int)(value: T) = new Series[T](Vector.fill[T](n)(value))
  def empty[T]: Series[T] = new Series[T](Vector.empty)
}
