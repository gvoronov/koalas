abstract class Field {
  val fieldType: String
  val column: String
}

case class Schema(val fields: List[Field]) {
  def apply(stringList: List[String]): Row = {
    if (fields.length != stringList.length)
      throw new RuntimeException("stringList length does not match schema length")
    Row(stringList.zip(fields).map(x => conApplyMap(x._1, x._2)).toMap)
  }

  private def conApplyMap(data: String, field: Field): Tuple2[String, DataValue] = {
    val value: DataValue = field.fieldType match {
      // eventaully fix so that "" get mapped to NumericalValue(NaN)
      case "Numerical" => NumericalValue(data.toDouble)
      case "SimpleCategorical" => CategoricalValue(data)
      case "ClassCategorical" =>
        CategoricalValue(data, field.asInstanceOf[CategoricalField].classCategory)
    }
    field.column -> value
  }
}

case class NumericalField(val column: String) extends Field {
  val fieldType = "Numerical"
}

case class CategoricalField(
    val column: String, val classCategory: String = null, categorySet: Set[String] = Set())
    extends Field {
  val fieldType = if (classCategory == null)
    "SimpleCategorical"
  else {
    if (! categorySet.isEmpty) {
      ClassCategoricalValue.setCategorySet(classCategory, categorySet)
    }
    "ClassCategorical"
  }
}
