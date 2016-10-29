package koalas.schema

import koalas.row.Row
import koalas.datavalue._

abstract class Field {
  val fieldType: String
  val column: String
}

case class Schema(val fields: List[Field]) {
  def apply(dataList: List[String]): Row = {
    if (fields.length != dataList.length)
      throw new RuntimeException("dataList length does not match schema length")
    Row(dataList.zip(fields).map(x => conApplyMap(x._1, x._2)).toMap)
  }
  def apply(dataMap: Map[String, String]): Row =
    Row(fields.map(field => conApplyMap(dataMap(field.column), field)).toMap)

  // At sompe point add methods for pre and post appending fields
  def insert(i: Int, field: Field): Schema = {
    val (head, tail) = fields.splitAt(i)
    Schema(head ++ List(field) ++ tail)
  }
  def columns: List[String] = fields.map(_.column)
  def length: Int = fields.length

  private def conApplyMap(data: String, field: Field): Tuple2[String, DataValue] = {
    val value: DataValue = field.fieldType match {
      // eventaully fix so that "" get mapped to NumericalValue(NaN)
      case "Numerical" => NumericalValue(data.toDouble)
      case "SimpleCategorical" => CategoricalValue(data)
      // Eventaully fix so that new classes get appeneded to the ClassCategoricalValue object
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
