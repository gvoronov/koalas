// Option 1
package datavalue

import math._

sealed abstract trait DataValue

class NumericalValue(val element: Double) extends DataValue {
  def apply(): Double = element

  def +(that: Double): NumericalValue = NumericalValue(element + that)
  def +(that: NumericalValue): NumericalValue = NumericalValue(element + that())

  def -(that: Double): NumericalValue = NumericalValue(element - that)
  def -(that: NumericalValue): NumericalValue = NumericalValue(element - that())

  def *(that: Double): NumericalValue = NumericalValue(element * that)
  def *(that: NumericalValue): NumericalValue = NumericalValue(element * that())

  def /(that: Double): NumericalValue = NumericalValue(element / that)
  def /(that: NumericalValue): NumericalValue = NumericalValue(element / that())

  def **(that: Double): NumericalValue = NumericalValue(pow(element, that))
  def **(that: NumericalValue): NumericalValue = NumericalValue(pow(element, that()))

  def >(that: Double): Boolean = element > that
  def >(that: NumericalValue): Boolean = element > that()

  def >=(that: Double): Boolean = element >= that
  def >=(that: NumericalValue): Boolean = element >= that()

  def <(that: Double): Boolean = element < that
  def <(that: NumericalValue): Boolean = element < that()

  def <=(that: Double): Boolean = element <= that
  def <=(that: NumericalValue): Boolean = element <= that()

  def ==(that: Double): Boolean = element == that
  def ==(that: NumericalValue): Boolean = element == that()

  def !=(that: Double): Boolean = element != that
  def !=(that: NumericalValue): Boolean = element != that()

  def ~=(that: Double): Boolean = (element - that).abs < NumericalValue.precision
  def ~=(that: NumericalValue): Boolean = (element - that()).abs < NumericalValue.precision

  def ~=(that: Double, precision: Double): Boolean = (element - that).abs < precision
  def ~=(that: NumericalValue, precision: Double): Boolean = (element - that()).abs < precision

  def unary_-: = NumericalValue(-element)
}

object NumericalValue {
  private var precision = 1.0E-6

  def apply(element: Double): NumericalValue = new NumericalValue(element)

  def setPrecision(newPrecision: Double): Unit = {precision = newPrecision}
  def getPrecision: Double = precision
}

sealed abstract trait CategoricalValue extends DataValue {
  def apply(): String

  def ==(that: String): Boolean = this() == that
  def ==(that: CategoricalValue): Boolean = this() == that()

  def !=(that: String): Boolean = this() != that
  def !=(that: CategoricalValue): Boolean = this() != that()
}

class SimpleCategoricalValue(val element: String) extends CategoricalValue {
  def apply(): String = element

  def ==(that: SimpleCategoricalValue): Boolean = element == that.element
  def !=(that: SimpleCategoricalValue): Boolean = element != that.element
  // def ==(other: String): Boolean = element == other
}

class ClassCategoricalValue(element: String, categoryClass: String)
    extends CategoricalValue {
  val category: Int = ClassCategoricalValue.classCategoryMap(categoryClass)(element)

  def apply(): String = ClassCategoricalValue.classCategoryStringMap(categoryClass)(category)

  def ==(that: ClassCategoricalValue): Boolean = category == that.category
  def !=(that: ClassCategoricalValue): Boolean = category != that.category
}

object CategoricalValue {
  def apply(element: String): SimpleCategoricalValue = new SimpleCategoricalValue(element)
  def apply(element: String, categoryClass: String): ClassCategoricalValue =
    new ClassCategoricalValue(element, categoryClass)
}

object ClassCategoricalValue {
  private var classCategoryMap: Map[String, Map[String, Int]] = Map()
  private var classCategoryStringMap: Map[String, Map[Int, String]] = Map()

  def setCategorySet(classCategory: String, categorySet: Set[String]): Unit = {
    classCategoryMap += (classCategory -> categorySet.zipWithIndex.toMap)
    classCategoryStringMap += (classCategory -> classCategoryMap(classCategory).map(_.swap))
  }
  // def appendCategorySet(classCategory: String, category: String): Unit =  {}
  def getCategorySet(classCategory: String): Set[String] = classCategoryMap(classCategory).keySet
}
