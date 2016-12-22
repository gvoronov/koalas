package koalas.schema

import koalas.row.Row
import koalas.datavalue._

abstract class Field {
  val fieldType: String
  val fieldName: String
}

final case class Schema(val fields: List[Field]) {
  lazy val nameToField: Map[String, Field] = fields.map(field => field.fieldName -> field).toMap

  def apply(dataList: List[String]): Row = {
    if (fields.length != dataList.length)
      throw new RuntimeException("dataList length does not match schema length")
    Row(dataList.zip(fields).map(x => conApplyMap(x._1, x._2)).toMap)
  }
  def apply(dataMap: Map[String, String]): Row =
    Row(fields.map(field => conApplyMap(dataMap(field.fieldName), field)).toMap)

  def select(columns: Iterable[String]): Schema =
    Schema(columns.map(column => nameToField(column)).toList)
  def select(columns: String*): Schema = select(columns.toIterable)

  // At sompe point add methods for pre and post appending fields
  def insert(i: Int, field: Field): Schema = {
    val (head, tail) = fields.splitAt(i)
    Schema(head ++ List(field) ++ tail)
  }
  def columns: List[String] = fields.map(_.fieldName)
  def length: Int = fields.length

  // Add functionality to select a field based on a name easily

  private def conApplyMap(data: String, field: Field): Tuple2[String, DataValue] = {
    val value: DataValue = field.fieldType match {
      // eventaully fix so that "" get mapped to NumericalValue(NaN)
      case "Numerical" => NumericalValue(data.toDouble)
      case "SimpleCategorical" => CategoricalValue(data)
      // Eventaully fix so that new classes get appeneded to the ClassCategoricalValue object
      case "ClassCategorical" =>
        CategoricalValue(data,
          field.asInstanceOf[CategoricalField].classCategory
            .getOrElse(throw new RuntimeException("fieldType improperly set!")))
    }
    field.fieldName -> value
  }
}

final case class NumericalField(val fieldName: String) extends Field {
  val fieldType = "Numerical"
}

final case class CategoricalField(
    val fieldName: String, val classCategory: Option[String] = None,
    categorySet: Set[String] = Set())
    extends Field {
  val fieldType = classCategory match {
    case None => "SimpleCategorical"
    case Some(classCategory) => {
      if (! categorySet.isEmpty) {
        ClassCategoricalValue.setCategorySet(classCategory, categorySet)
      }
      "ClassCategorical"
    }
  }
  // val fieldType = if (classCategory.isEmpty)
  //   "SimpleCategorical"
  // else {
  //   if (! categorySet.isEmpty) {
  //     ClassCategoricalValue.setCategorySet(classCategory, categorySet)
  //   }
  //   "ClassCategorical"
  // }
}
