package koalas.datavalue

import math._

abstract class DataValue

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

final object NumericalValue {
  private var precision = 1.0E-6

  def apply(element: Double): NumericalValue = new NumericalValue(element)

  def setPrecision(newPrecision: Double): Unit = {precision = newPrecision}
  def getPrecision: Double = precision
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

final object CategoricalValue {
  def apply(element: String): SimpleCategoricalValue = new SimpleCategoricalValue(element)
  def apply(element: String, categoryClass: String): ClassCategoricalValue =
    new ClassCategoricalValue(element, categoryClass)
}

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
