// Option 1
abstract class DataElement
case class Numerical(val element: Double) extends DataElement {
  def apply(): Double = element
}
case class Categorical(val element: String) extends DataElement {
  def apply(): String = element
}

// Option 2
abstract sealed trait DataElement {
  val numerical: Double
  val categorical: String

  def isNumerical(): Boolean = ! numerical.equals(Double.NaN)
  def isCategorical(): Boolean = !(categorical == null)
}
case class Numerical(val numerical: Double) extends DataElement {
  val categorical: String = null
  def apply(): Double = numerical
}
case class Categorical(val categorical: String) extends DataElement {
  val numerical: Double = Double.NaN
  def apply(): String = categorical
}

// Option 3
class DataElement[T](val element: T) {
  def apply(): T = element
}
class Numerical(element: Double) extends DataElement[Double](element)
class Categorical(element: String) extends DataElement[String](element)
