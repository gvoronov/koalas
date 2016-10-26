// Option 1
package datavalue

import math._

sealed abstract trait DataValue

case class NumericalValue(val element: Double) extends DataValue {
  def apply(): Double = element

  def +(x: Double): NumericalValue = NumericalValue(element + x)
  def +(x: NumericalValue): NumericalValue = NumericalValue(element + x())

  def -(x: Double): NumericalValue = NumericalValue(element - x)
  def -(x: NumericalValue): NumericalValue = NumericalValue(element - x())

  def *(x: Double): NumericalValue = NumericalValue(element * x)
  def *(x: NumericalValue): NumericalValue = NumericalValue(element * x())

  def /(x: Double): NumericalValue = NumericalValue(element / x)
  def /(x: NumericalValue): NumericalValue = NumericalValue(element / x())

  def **(x: Double): NumericalValue = NumericalValue(pow(element, x))
  def **(x: NumericalValue): NumericalValue = NumericalValue(pow(element, x()))

  def unary_-: = NumericalValue(-element)
}

sealed abstract trait CategoricalValue extends DataValue

class SimpleCategoricalValue(val element: String) extends CategoricalValue {
  def apply(): String = element
}

class ClassCategoricalValue(element: String, categoryClass: String)
  extends CategoricalValue {
    val category: Int = ClassCategoricalValue.classCategoryMap(categoryClass)(element)

    def apply(): String = ClassCategoricalValue.classCategoryStringMap(categoryClass)(category)
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
