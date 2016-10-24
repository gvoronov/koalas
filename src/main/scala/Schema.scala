abstract class Field {
  val type: String
  val column: String
}

case class Schema(val fields: List[Field]) {
  // def apply(stringList: List[String]): Row = {}
}

case class NumericalField(val column: String) extends Field {
  val type = "Numerical"
}

case class CategoricalField(
    column: String, classCategory: String = null, categorySet: Set[String] = Set())
    extends Field {
  if classCategory == null {
    type = "SimpleCategorical"
  } else {
    type = "ClassCategorical"
    if ! categorySet.isEmpty {
      ClassCategoricalValue.setCategorySet(classCategory, categorySet)
    }
  }
}
