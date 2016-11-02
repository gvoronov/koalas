package koalas.datavalue

import math._

abstract class DataValue {
  def apply(): Any
  override def toString: String = this().toString
}

final class NumericalValue(val element: Double) extends DataValue {
  def apply(): Double = element

  override def hashCode: Int = ("Numerical", element).hashCode
  override def equals(that: Any): Boolean = {
    that match {
      case _: NumericalValue => this.hashCode == that.hashCode
      case _: Double => element equals that.asInstanceOf[Double]
      case _: Int => element equals that.asInstanceOf[Int].toDouble
      case _ => throw new RuntimeException("Can't evaluate equals")
    }
  }

  private def anyToDouble(that: Any): Double = {
    that match {
      case _: NumericalValue => that.asInstanceOf[NumericalValue]()
      case _: Double => that.asInstanceOf[Double]
      case _: Int => that.asInstanceOf[Int].toDouble
      case _ => throw new RuntimeException("Can't recast operand, that, as a Double!")
    }
  }

  def +(that: Any): NumericalValue = NumericalValue(element + anyToDouble(that))
  def -(that: Any): NumericalValue = NumericalValue(element - anyToDouble(that))
  def *(that: Any): NumericalValue = NumericalValue(element * anyToDouble(that))
  def /(that: Any): NumericalValue = NumericalValue(element / anyToDouble(that))
  def **(that: Any): NumericalValue = NumericalValue(pow(element, anyToDouble(that)))

  def >(that: Any): Boolean = element > anyToDouble(that)
  def >=(that: Any): Boolean = element >= anyToDouble(that)
  def <(that: Any): Boolean = element < anyToDouble(that)
  def <=(that: Any): Boolean = element <= anyToDouble(that)

  def ~=(that: Any): Boolean = (element - anyToDouble(that)).abs < NumericalValue.precision
  def ~=(that: Any, precision: Double): Boolean = (element - anyToDouble(that)).abs < precision

  def unary_-: = NumericalValue(-element)
}

/** Factory for [[koalas.datavalue.NumericalValue]] instances. */
final object NumericalValue {
  private var precision = 1.0E-6

  def apply(element: Double): NumericalValue = new NumericalValue(element)

  def setPrecision(newPrecision: Double): Unit = {precision = newPrecision}
  def getPrecision: Double = precision
}

/**
 * This trait can be used to define implicit object so that collections of NumericalValue's can be
 * summed via collection sum method.
 */
trait NumericNV extends Numeric[NumericalValue] {
  // Members declared in scala.math.Numeric
  def fromInt(x: Int): NumericalValue = NumericalValue(x)
  def minus(x: NumericalValue, y: NumericalValue): NumericalValue = x - y
  def negate(x: NumericalValue): NumericalValue = -x
  def plus(x: NumericalValue, y: NumericalValue): NumericalValue = x + y
  def times(x: NumericalValue, y: NumericalValue): NumericalValue = x * y
  def toDouble(x: NumericalValue): Double = x()
  def toFloat(x: NumericalValue): Float = x().toFloat
  def toInt(x: NumericalValue): Int = x().toInt
  def toLong(x: NumericalValue): Long = x().toLong

  // Members declared in scala.math.Ordering
  def compare(x: NumericalValue, y: NumericalValue): Int = x() compare y()
}

abstract class CategoricalValue extends DataValue {
  def apply(): String
}

final class SimpleCategoricalValue(val element: String) extends CategoricalValue {
  def apply(): String = element

  override def hashCode: Int = ("SimpleCategorical", element).hashCode
  override def equals(that: Any): Boolean =
    that.isInstanceOf[SimpleCategoricalValue] && this.hashCode == that.hashCode
}

final class ClassCategoricalValue(element: String, categoryClass: String)
    extends CategoricalValue {
  val category: Int = ClassCategoricalValue.classCategoryMap(categoryClass)(element)

  def apply(): String = ClassCategoricalValue.classCategoryStringMap(categoryClass)(category)

  override def hashCode: Int = ("ClassCategorical", category).hashCode
  override def equals(that: Any): Boolean =
    that.isInstanceOf[ClassCategoricalValue] && this.hashCode == that.hashCode
}

/**
 * Factory for [[koalas.datavalue.SimpleCategoricalValue]] and
 * [[koalas.datavalue.ClassCategoricalValue]] instances.
 */
final object CategoricalValue {
  def apply(element: String): SimpleCategoricalValue = new SimpleCategoricalValue(element)
  def apply(element: String, categoryClass: String): ClassCategoricalValue =
    new ClassCategoricalValue(element, categoryClass)
}

/**
 * Companion object but not factory for [[koalas.datavalue.ClassCategoricalValue]] instances. THis
 * object mangages category labels for class values where all labels are known, so individual
 * instaNces don't have to.
 */
final object ClassCategoricalValue {
  private var classCategoryMap: Map[String, Map[String, Int]] = Map()
  private var classCategoryStringMap: Map[String, Map[Int, String]] = Map()

  def setCategorySet(classCategory: String, categorySet: Set[String]): Unit = {
    classCategoryMap += (classCategory -> categorySet.zipWithIndex.toMap)
    classCategoryStringMap += (classCategory -> classCategoryMap(classCategory).map(_.swap))
  }
  // def appendCategorySet(classCategory: String, category: String): Unit =  {}
  def getCategorySet(classCategory: String): Set[String] = classCategoryMap(classCategory).keySet
}
