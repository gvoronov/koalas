sealed trait DataElement
case class Numerical(val element: Double) extends DataElement {
  def apply(): Double = element
}
case class Categorical(val element: String) extends DataElement {
  def apply(): String = element
}


// sealed trait DataElement[Double, String]
// case class Numerical[Double, String](a: Double) extends DataElement[Double, String]
// case class Categorical[Double, String](b: String) extends DataElement[Double, String]
