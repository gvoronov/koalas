// Option 1
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

// Investigate using a companion object to maintain list of possible categories
case class CategoricalValue(val element: String) extends DataValue {
  def apply(): String = element
}
// case class DateValue()

// Option 2
// abstract sealed trait DataElement {
//   val numerical: Double
//   val categorical: String
//
//   def isNumerical(): Boolean = ! numerical.equals(Double.NaN)
//   def isCategorical(): Boolean = !(categorical == null)
// }
// case class Numerical(val numerical: Double) extends DataElement {
//   val categorical: String = null
//   def apply(): Double = numerical
// }
// case class Categorical(val categorical: String) extends DataElement {
//   val numerical: Double = Double.NaN
//   def apply(): String = categorical
// }

// Option 3
// class DataElement[T](val element: T) {
//   def apply(): T = element
// }
// class Numerical(element: Double) extends DataElement[Double](element)
// class Categorical(element: String) extends DataElement[String](element)
