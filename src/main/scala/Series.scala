package koalas.series

// import shapeless.TypeCase

import koalas.datavalue._
import koalas.numericalops.NumericalOps

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

  lazy val last: T = values.last

  private def binaryOpOnAny[D, R](that: Any, op: (D, D) => R): Series[R] = {
    that match {
      // case series: Series[Any] => series(0) match {
      //   case element: NumericalValue => {
      //     assert(last.isInstanceOf[NumericalValue])
      //     Series(values.zip(series.values).map(pair => op(pair._1.asInstanceOf[D], pair._2.asInstanceOf[D])))
      //   }
      //   case _ => throw new RuntimeException("")
      // }
      case series: Series[Any] => {
        series(0) match {
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


      // case value: NumericalValue => last match {
      //   case _: NumericalValue => Series(values.map(y => op(y.asInstanceOf[D], value.asInstanceOf[D])))
      //   case _: Double => Series(values.map(y => op(y.asInstanceOf[D], value().asInstanceOf[D])))
      //   case _: Int => Series(values.map(y => op(y.asInstanceOf[D], value().asInstanceOf[D])))
      //   case _ => throw new RuntimeException("")
      // }
      // case value: Double => last match {
      //   case _: NumericalValue => Series(values.map(y => op(y.asInstanceOf[D], NumericalValue(value).asInstanceOf[D])))
      //   case _: Double | _: Int => Series(values.map(y => op(y.asInstanceOf[D], value.asInstanceOf[D])))
      //   case _: Int => Series(values.map(y => op(y.asInstanceOf[D], value.asInstanceOf[D])))
      //   case _ => throw new RuntimeException("")
      // }
      case value: NumericalValue => last match {
        case _: NumericalValue => Series(values.map(y => op(y.asInstanceOf[D], value.asInstanceOf[D])))
        case _: Double | _: Int => Series(values.map(y => op(y.asInstanceOf[D], value().asInstanceOf[D])))
        case _ => throw new RuntimeException("")
      }
      case _: Double | _: Int => last match {
        case _: NumericalValue => Series(values.map(y => op(y.asInstanceOf[D], NumericalValue(that.asInstanceOf[Double]).asInstanceOf[D])))
        case _: Double | _: Int => Series(values.map(y => op(y.asInstanceOf[D], that.asInstanceOf[D])))
        case _ => throw new RuntimeException("")
      }
      // case value: Double => Series(values.map(y => op(y.asInstanceOf[D], NumericalValue(value).asInstanceOf[D])))
      // case value: Int => Series(values.map(y => op(y.asInstanceOf[D], NumericalValue(value).asInstanceOf[D])))
      case _ => throw new RuntimeException(
        "Series binary opertion attempted with non-numerical type")
    }
  }

  /**
   * This method will check that self type matches that of right operatnd. Following this, it will
   * pattern match onto the the appropriate domaon type of the binary op.
   */
  // private def selectBinaryOpInType[A](that: Any):
  // def +(that: Any): Series[NumericalValue] = binaryOpOnAny[NumericalValue, NumericalValue](that, (a, b) => a + b)
  //
  // trait NumericLike[A] {
  //   def +(that: A): A
  //   // def +(that: NumericalValue): NumericalValue = this + that
  // }
  // type A = T with NumericLike[T]

  // def sum[B >: T](implicit num: Numeric[B], a: B, b: B): B  = num.plus(a, b)


  // def binaryOp
  // def +(that: Any): Series[T] = binaryOpOnAny[A, A](that, (a: A, b: A) => (a + b).asInstanceOf[A]).asInstanceOf[Series[T]]

  def +[B >: T](that: Any)(implicit num: Numeric[B]):Series[T] =
    binaryOpOnAny[B, B](that, (a, b) => num.plus(a, b)).asInstanceOf[Series[T]]
  // def /[B >: T](that: Any)(implicit num: NumericalOps[B]):Series[T] =
  //   binaryOpOnAny[B, B](that, (a, b) => num.divide(a, b)).asInstanceOf[Series[T]]
  // def +(that: Any): Series[T] = {
  //   last match {
  //     case _: NumericalValue =>
  //       binaryOpOnAny[NumericalValue, NumericalValue](that, (a, b) => a + b).asInstanceOf[Series[T]]
  //     case _: Double =>
  //       binaryOpOnAny[Double, Double](that, (a, b) => a + b).asInstanceOf[Series[T]]
  //     case _: Int =>
  //       binaryOpOnAny[Int, Int](that, (a, b) => a + b).asInstanceOf[Series[T]]
  //     case _ => throw new RuntimeException("")
  //   }
  // }
  def -(that: Any): Series[T] = {
    last match {
      case _: NumericalValue =>
        binaryOpOnAny[NumericalValue, NumericalValue](that, (a, b) => a - b).asInstanceOf[Series[T]]
      case _: Double =>
        binaryOpOnAny[Double, Double](that, (a, b) => a - b).asInstanceOf[Series[T]]
      case _: Int =>
        binaryOpOnAny[Int, Int](that, (a, b) => a - b).asInstanceOf[Series[T]]
      case _ => throw new RuntimeException("")
    }
  }
  def *(that: Any): Series[T] = {
    last match {
      case _: NumericalValue =>
        binaryOpOnAny[NumericalValue, NumericalValue](that, (a, b) => a * b).asInstanceOf[Series[T]]
      case _: Double =>
        binaryOpOnAny[Double, Double](that, (a, b) => a * b).asInstanceOf[Series[T]]
      case _: Int =>
        binaryOpOnAny[Int, Int](that, (a, b) => a * b).asInstanceOf[Series[T]]
      case _ => throw new RuntimeException("")
    }
  }
  def /(that: Any): Series[T] = {
    last match {
      case _: NumericalValue =>
        binaryOpOnAny[NumericalValue, NumericalValue](that, (a, b) => a / b).asInstanceOf[Series[T]]
      case _: Double =>
        binaryOpOnAny[Double, Double](that, (a, b) => a / b).asInstanceOf[Series[T]]
      case _: Int =>
        binaryOpOnAny[Int, Int](that, (a, b) => a / b).asInstanceOf[Series[T]]
      case _ => throw new RuntimeException("")
    }
  }
  // def -(that: Any): Series[NumericalValue] = binaryOpOnAny[NumericalValue, NumericalValue](that, (a, b) => a - b)
  // def *(that: Any): Series[NumericalValue] = binaryOpOnAny[NumericalValue, NumericalValue](that, (a, b) => a * b)
  // def /(that: Any): Series[NumericalValue] = binaryOpOnAny[NumericalValue, NumericalValue](that, (a, b) => a / b)
  def **(that: Any): Series[NumericalValue] = binaryOpOnAny[NumericalValue, NumericalValue](that, (a, b) => a ** b)

  def :>(that: Any): Series[Boolean] = binaryOpOnAny[NumericalValue, Boolean](that, (a, b) => a > b)
  def :>=(that: Any): Series[Boolean] = binaryOpOnAny[NumericalValue, Boolean](that, (a, b) => a >= b)
  def :<(that: Any): Series[Boolean] = binaryOpOnAny[NumericalValue, Boolean](that, (a, b) => a < b)
  def :<=(that: Any): Series[Boolean] = binaryOpOnAny[NumericalValue, Boolean](that, (a, b) => a <= b)
  def :~=(that: Any): Series[Boolean] = binaryOpOnAny[NumericalValue, Boolean](that, (a, b) => a ~= b)
  def :~=(that: Any, precision: Double): Series[Boolean] = binaryOpOnAny[NumericalValue, Boolean](that, (a, b) => a ~= (b, precision))
  def :==(that: Any): Series[Boolean] = binaryOpOnAny[Any, Boolean](that, (a, b) => a equals b)
  def :!=(that: Any): Series[Boolean] = binaryOpOnAny[Any, Boolean](that, (a, b) => !(a equals b))

  override def toString: String = values.toString
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
  def apply[T](values: T*): Series[T] = new Series[T](values.toVector)
}
