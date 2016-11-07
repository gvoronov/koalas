package koalas.series

import koalas.datavalue._
import koalas.numericalops._
import koalas.numericalops.NumericalOpsImps._

class Series[+T](val values: Vector[T]){
  // mabye do with
  // private val mySummary: MutableMap[String, Any] = MutableMap.empty
  private var mySum: Option[Any] = None
  private var myMean: Option[Any] = None
  private var myX1: Option[Any] = None
  private var myX2: Option[Any] = None
  private var myVariance: Option[Any] = None

  lazy val length: Int = values.length
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

  override def toString: String = values.toString

  lazy val last: T = values.last

  private def binaryOpOnAny[D, R](that: Any, op: (D, D) => R): Series[R] = {
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

  def sum[B >: T](implicit num: Numeric[B]): B = mySum.getOrElse({
    mySum = Some(values.reduce(num.plus))
    mySum.get
  }).asInstanceOf[B]
  def mean[A >: T](implicit num: Fractional[A]): A = myMean.getOrElse({
    myMean = Some(num.div(sum(num), num.fromInt(length)))
    myMean.get
  }).asInstanceOf[A]
  def moment1[A >: T](implicit num: Fractional[A]): A = myX1.getOrElse({
    myX1 = Some(mean(num))
    myX1.get
  }).asInstanceOf[A]
  def moment2[A >: T](implicit num: Fractional[A] with Exponentialable[A]): A = myX2.getOrElse({
    myX2 = Some(num.div(map(num.pow(_, num.fromInt(2))).reduce(num.plus), num.fromInt(length)))
    myX2.get
  }).asInstanceOf[A]
  def variance[A >: T](implicit num: Fractional[A] with Exponentialable[A]): A = myVariance.getOrElse({
    myVariance = Some(num.minus(moment2(num), num.pow(moment1(num), num.fromInt(2))))
    myVariance.get
  }).asInstanceOf[A]
}

object Series{
  def apply[T](values: Vector[T]): Series[T] = new Series[T](values)
  def apply[T](values: Iterable[T]): Series[T] = new Series[T](values.toVector)
  def apply[T](values: T*): Series[T] = new Series[T](values.toVector)
}
