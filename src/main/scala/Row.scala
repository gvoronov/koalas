package koalas.row

import koalas.datavalue._
import koalas.schema._

case class Row(val paramMap: Map[String, DataValue]){
  def apply[T](column: String): T = paramMap(column).asInstanceOf[T]

  def select(column: String): DataValue = paramMap(column)
  def select(columns: String*): Row = Row(paramMap filterKeys columns.toSet)
  def select(columns: Set[String]): Row = Row(paramMap filterKeys columns)
  def select(columns: Iterable[String]): Row = Row(paramMap filterKeys columns.toSet)

  def +(kvPair: Tuple2[String, DataValue]): Row = Row(paramMap + kvPair)
  def +(otherRow: Row): Row = Row(paramMap ++ otherRow.paramMap)

  def update(column: String, value: DataValue): Row = Row(paramMap + (column -> value))

  def getSchema: Schema = {
    def rowColToField(column: Tuple2[String, DataValue]): Field = {
      column._2 match {
        case value: NumericalValue => NumericalField(column._1)
        case value: SimpleCategoricalValue => CategoricalField(column._1)
        case value: ClassCategoricalValue => CategoricalField(
          column._1, Some(value.categoryClass),
          ClassCategoricalValue.getCategorySet(value.categoryClass))
      }
    }
    Schema(paramMap.map(rowColToField).toList)
    // for ((column, dataValue) <- paramMap) {
    //
    // }
  }
}
